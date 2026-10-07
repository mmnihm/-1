import 'dotenv/config';
import mysql from 'mysql2/promise';
import { Telegraf, Markup } from 'telegraf';

const {
  BOT_TOKEN,
  ADMIN_ID,
  MYSQL_HOST,
  MYSQL_PORT = '3306',
  MYSQL_DATABASE,
  MYSQL_USER,
  MYSQL_PASSWORD
} = process.env;

if (!BOT_TOKEN) throw new Error('缺少 BOT_TOKEN');
if (!ADMIN_ID) throw new Error('缺少 ADMIN_ID');

const adminId = Number(ADMIN_ID);
const bot = new Telegraf(BOT_TOKEN);

let pool = null;
const sessions = new Map();

async function db() {
  if (!pool) {
    pool = mysql.createPool({
      host: MYSQL_HOST,
      port: Number(MYSQL_PORT),
      database: MYSQL_DATABASE,
      user: MYSQL_USER,
      password: MYSQL_PASSWORD,
      waitForConnections: true,
      connectionLimit: 5,
      charset: 'utf8mb4'
    });
    await pool.query(`
      CREATE TABLE IF NOT EXISTS forward_tasks (
        id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        admin_id BIGINT NOT NULL,
        source_chat_id BIGINT NOT NULL,
        target_chat_id BIGINT NOT NULL,
        source_message_id BIGINT DEFAULT 0,
        status VARCHAR(20) NOT NULL DEFAULT 'paused',
        realtime TINYINT(1) NOT NULL DEFAULT 1,
        history_next_id BIGINT DEFAULT 0,
        history_end_id BIGINT DEFAULT 0,
        history_done TINYINT(1) NOT NULL DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uq_task (admin_id, source_chat_id, target_chat_id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    `);
    for (const sql of [
      'ALTER TABLE forward_tasks ADD COLUMN history_next_id BIGINT DEFAULT 0',
      'ALTER TABLE forward_tasks ADD COLUMN history_end_id BIGINT DEFAULT 0',
      'ALTER TABLE forward_tasks ADD COLUMN history_done TINYINT(1) NOT NULL DEFAULT 0'
    ]) {
      try { await pool.query(sql); } catch (e) {
        if (e?.code !== 'ER_DUP_FIELDNAME') throw e;
      }
    }
  }
  return pool;
}

function isAdmin(ctx) {
  return Number(ctx.from?.id) === adminId;
}

function menu() {
  return Markup.inlineKeyboard([
    [Markup.button.callback('➕ 添加任务', 'add_task')],
    [Markup.button.callback('🕘 设置历史范围', 'set_history')],
    [Markup.button.callback('▶️ 开始同步', 'start_sync'), Markup.button.callback('⏸ 暂停同步', 'pause_sync')],
    [Markup.button.callback('🔄 实时转发', 'realtime')],
    [Markup.button.callback('📋 我的任务', 'tasks'), Markup.button.callback('🗑 删除任务', 'delete_task')]
  ]);
}

function cleanChatId(value) {
  const s = String(value || '').trim();
  if (/^-?\d+$/.test(s)) return Number(s);
  const m = s.match(/(?:t\.me\\/|@)([A-Za-z0-9_]+)/);
  return m ? '@' + m[1] : s;
}

async function getTasks() {
  const p = await db();
  const [rows] = await p.query(
    'SELECT * FROM forward_tasks WHERE admin_id=? ORDER BY id DESC',
    [adminId]
  );
  return rows;
}

async function syncTask(task) {
  const p = await db();
  let nextId = Number(task.history_next_id || 0);
  const endId = Number(task.history_end_id || 0);
  if (!nextId || !endId || nextId > endId) return;
  while (nextId <= endId) {
    const batchEnd = Math.min(nextId + 9, endId);
    for (let id = nextId; id <= batchEnd; id++) {
      const [rows] = await p.query('SELECT status FROM forward_tasks WHERE id=?', [task.id]);
    if (!rows.length || rows[0].status !== 'running') return;
    try {
      await copyWithRetry(task.source_chat_id, task.target_chat_id, id);
      await p.query('UPDATE forward_tasks SET history_next_id=? WHERE id=?', [id + 1, task.id]);
    } catch (err) {
      if (isNotFoundMessage(err)) {
        await p.query('UPDATE forward_tasks SET history_next_id=? WHERE id=?', [id + 1, task.id]);
        continue;
      }
      console.error('历史同步失败', task.id, id, err?.message || err);
      await p.query('UPDATE forward_tasks SET status="paused" WHERE id=?', [task.id]);
      return;
      }
      nextId = batchEnd + 1;
    }
  }
  await p.query('UPDATE forward_tasks SET history_done=1,status="paused" WHERE id=?', [task.id]);
}

function isNotFoundMessage(err) {
  const code = Number(err?.response?.error_code || 0);
  const desc = String(err?.response?.description || err?.message || '');
  return code === 400 && /message to copy not found|message_id_invalid|message not found/i.test(desc);
}

async function startHistoryJobs() {
  const p = await db();
  const [tasks] = await p.query(
    'SELECT * FROM forward_tasks WHERE admin_id=? AND status="running" AND history_done=0',
    [adminId]
  );
  for (const task of tasks) {
    syncTask(task).catch(err => console.error('历史任务异常', task.id, err));
  }
}

async function showTasks(ctx) {
  const rows = await getTasks();
  if (!rows.length) {
    return ctx.reply('📋 目前没有转发任务。', menu());
  }
  const lines = rows.map(t =>
    `#${t.id}\\n源：${t.source_chat_id}\\n目标：${t.target_chat_id}\\n状态：${t.status}\\n断点：${t.source_message_id || 0}`
  );
  return ctx.reply('📋 转发任务\\n\\n' + lines.join('\\n\\n'), menu());
}

bot.start(async ctx => {
  if (!isAdmin(ctx)) return ctx.reply('机器人已运行。');
  return ctx.reply('🤖 转发机器人\\n\\n请选择操作：', menu());
});

bot.command('menu', async ctx => {
  if (!isAdmin(ctx)) return;
  return ctx.reply('🤖 主菜单', menu());
});

bot.action('add_task', async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery();
  sessions.set(ctx.from.id, { step: 'source' });
  await ctx.answerCbQuery();
  return ctx.reply('➕ 添加转发任务\\n\\n请发送【源频道/群】的 ID、@用户名或 t.me 链接。');
});

bot.action('start_sync', async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery();
  const p = await db();
  const [r] = await p.query(
    'UPDATE forward_tasks SET status="running" WHERE admin_id=? AND target_chat_id<>source_chat_id',
    [adminId]
  );
  await startHistoryJobs();
  await ctx.answerCbQuery('已开始');
  return ctx.reply(`▶️ 已启动 ${r.affectedRows} 个同步任务。\\n历史消息同步模块将按断点继续。`, menu());
});

bot.action('pause_sync', async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery();
  const p = await db();
  await p.query('UPDATE forward_tasks SET status="paused" WHERE admin_id=?', [adminId]);
  await ctx.answerCbQuery('已暂停');
  return ctx.reply('⏸ 所有同步任务已暂停。', menu());
});

bot.action('realtime', async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery();
  const p = await db();
  await p.query('UPDATE forward_tasks SET realtime=1 WHERE admin_id=?', [adminId]);
  await ctx.answerCbQuery('已开启');
  return ctx.reply('🔄 实时转发已开启。', menu());
});

bot.action('tasks', async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery();
  await ctx.answerCbQuery();
  return showTasks(ctx);
});

bot.action('delete_task', async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery();
  const rows = await getTasks();
  if (!rows.length) return ctx.answerCbQuery('没有任务');
  await ctx.answerCbQuery();
  return ctx.reply(
    '🗑 请选择要删除的任务：',
    Markup.inlineKeyboard(rows.map(t => [
      Markup.button.callback(`删除 #${t.id}`, `del_${t.id}`)
    ]))
  );
});

bot.action(/^del_(\d+)$/, async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery();
  const taskId = Number(ctx.match[1]);
  const p = await db();
  const [r] = await p.query(
    'DELETE FROM forward_tasks WHERE id=? AND admin_id=?',
    [taskId, adminId]
  );
  await ctx.answerCbQuery(r.affectedRows ? '已删除' : '任务不存在');
  return ctx.reply(r.affectedRows ? `🗑 任务 #${taskId} 已删除。` : '⚠️ 任务不存在。', menu());
});

bot.action('set_history', async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery();
  const rows = await getTasks();
  if (!rows.length) return ctx.answerCbQuery('没有任务');
  sessions.set(ctx.from.id, { step: 'history_task', taskId: rows[0].id });
  await ctx.answerCbQuery();
  return ctx.reply('请发送任务编号和历史结束消息 ID，例如：1 5000');
});

bot.on('text', async ctx => {
  if (!isAdmin(ctx)) return;
  const session = sessions.get(ctx.from.id);
  if (!session) return;

  const chat = cleanChatId(ctx.message.text);

  if (session.step === 'history_task') {
    const parts = String(ctx.message.text).trim().split(/\s+/);
    const taskId = Number(parts[0]);
    const endId = Number(parts[1]);
    if (!Number.isInteger(taskId) || !Number.isInteger(endId) || endId < 1) {
      return ctx.reply('格式错误，请发送：任务编号 结束消息ID，例如：1 5000');
    }
    const p = await db();
    const [rows] = await p.query('SELECT * FROM forward_tasks WHERE id=? AND admin_id=?', [taskId, adminId]);
    if (!rows.length) return ctx.reply('❌ 找不到这个任务。');
    await p.query(
      'UPDATE forward_tasks SET history_next_id=IF(history_next_id=0,1,history_next_id), history_end_id=?, history_done=0 WHERE id=?',
      [endId, taskId]
    );
    sessions.delete(ctx.from.id);
    return ctx.reply(`✅ 已设置任务 #${taskId} 的历史结束 ID：${endId}\\n现在点击“▶️ 开始同步”。`, menu());
  }

  if (session.step === 'source') {
    session.source = chat;
    session.step = 'target';
    return ctx.reply(`✅ 源频道已记录：${chat}\\n\\n现在请发送【目标频道/群】的 ID、@用户名或 t.me 链接。`);
  }

  if (session.step === 'target') {
    if (String(chat) === String(session.source)) {
      return ctx.reply('❌ 源频道和目标频道不能相同。\\n请重新发送目标频道/群。');
    }

    const p = await db();
    await p.query(
      `INSERT INTO forward_tasks
       (admin_id, source_chat_id, target_chat_id, status, realtime)
       VALUES (?, ?, ?, 'paused', 1)
       ON DUPLICATE KEY UPDATE updated_at=CURRENT_TIMESTAMP`,
      [adminId, session.source, chat]
    );

    const source = session.source;
    sessions.delete(ctx.from.id);
    return ctx.reply(
      `✅ 转发任务已添加\\n\\n源：${source}\\n目标：${chat}\\n\\n现在点击“▶️ 开始同步”才会真正开始历史同步。`,
      menu()
    );
  }
});

bot.on('channel_post', async ctx => {
  try {
    const sourceId = ctx.chat.id;
    const p = await db();
    const [tasks] = await p.query(
      'SELECT * FROM forward_tasks WHERE source_chat_id=? AND realtime=1 AND status<>"deleted"',
      [sourceId]
    );

    for (const task of tasks) {
      if (task.target_chat_id === task.source_chat_id) continue;
      try {
        await copyWithRetry(sourceId, task.target_chat_id, ctx.channelPost.message_id);
        await p.query(
          'UPDATE forward_tasks SET source_message_id=? WHERE id=?',
          [ctx.channelPost.message_id, task.id]
        );
      } catch (err) {
        console.error('实时转发失败', task.id, err?.message || err);
      }
    }
  } catch (err) {
    console.error('channel_post handler', err);
  }
});

async function copyWithRetry(source, target, messageId) {
  for (;;) {
    try {
      return await bot.telegram.copyMessage(target, source, messageId);
    } catch (err) {
      const retryAfter = Number(
        err?.response?.parameters?.retry_after ||
        err?.parameters?.retry_after ||
        0
      );
      if (retryAfter > 0) {
        await new Promise(r => setTimeout(r, retryAfter * 1000 + 500));
        continue;
      }
      throw err;
    }
  }
}

bot.catch(err => console.error('BOT ERROR:', err));

(async () => {
  await db();
  await bot.launch();
  console.log('Telegram 转发机器人已启动');
})();

process.once('SIGINT', () => bot.stop('SIGINT'));
process.once('SIGTERM', () => bot.stop('SIGTERM'));
