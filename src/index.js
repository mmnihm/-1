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
if (!MYSQL_HOST) throw new Error('缺少 MYSQL_HOST');
if (!MYSQL_DATABASE) throw new Error('缺少 MYSQL_DATABASE');
if (!MYSQL_USER) throw new Error('缺少 MYSQL_USER');
if (!MYSQL_PASSWORD) throw new Error('缺少 MYSQL_PASSWORD');

const adminId = Number(ADMIN_ID);
const bot = new Telegraf(BOT_TOKEN);
let pool = null;
const sessions = new Map();
const runningJobs = new Set();
const forwardingLocks = new Set();

const DEFAULT_FILTERS = {
  text: true,
  photo: true,
  video: true,
  document: true,
  audio: true,
  voice: true,
  animation: true,
  sticker: true,
  video_note: true,
  other: true
};

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
        history_total BIGINT DEFAULT 0,
        history_processed BIGINT DEFAULT 0,
        history_skipped BIGINT DEFAULT 0,
        history_failed BIGINT DEFAULT 0,
        filters_json TEXT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uq_task (admin_id, source_chat_id, target_chat_id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    `);

    const migrations = [
      'ALTER TABLE forward_tasks ADD COLUMN history_next_id BIGINT DEFAULT 0',
      'ALTER TABLE forward_tasks ADD COLUMN history_end_id BIGINT DEFAULT 0',
      'ALTER TABLE forward_tasks ADD COLUMN history_done TINYINT(1) NOT NULL DEFAULT 0',
      'ALTER TABLE forward_tasks ADD COLUMN history_total BIGINT DEFAULT 0',
      'ALTER TABLE forward_tasks ADD COLUMN history_processed BIGINT DEFAULT 0',
      'ALTER TABLE forward_tasks ADD COLUMN history_skipped BIGINT DEFAULT 0',
      'ALTER TABLE forward_tasks ADD COLUMN history_failed BIGINT DEFAULT 0',
      'ALTER TABLE forward_tasks ADD COLUMN filters_json TEXT NULL'
    ];
    for (const sql of migrations) {
      try { await pool.query(sql); }
      catch (e) {
        if (e?.code !== 'ER_DUP_FIELDNAME') throw e;
      }
    }

    await pool.query(`
      CREATE TABLE IF NOT EXISTS forwarded_messages (
        id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        task_id BIGINT UNSIGNED NOT NULL,
        source_message_id BIGINT NOT NULL,
        target_message_id BIGINT DEFAULT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uq_forwarded (task_id, source_message_id),
        KEY idx_task (task_id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    `);
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
    [Markup.button.callback('🎛 过滤设置', 'filters'), Markup.button.callback('📊 任务进度', 'progress')],
    [Markup.button.callback('📋 我的任务', 'tasks'), Markup.button.callback('🗑 删除任务', 'delete_task')]
  ]);
}

function cleanChatId(value) {
  const s = String(value || '').trim();
  if (/^-?\d+$/.test(s)) return Number(s);
  const m = s.match(/(?:https?:\/\/)?(?:t\.me\/|@)([A-Za-z0-9_]+)/i);
  return m ? '@' + m[1] : s;
}

async function resolveChatId(value) {
  const cleaned = cleanChatId(value);
  if (typeof cleaned === 'number') return cleaned;
  if (!cleaned) throw new Error('频道/群不能为空');
  const chat = await bot.telegram.getChat(cleaned);
  if (!chat?.id) throw new Error('无法获取频道/群 ID');
  return Number(chat.id);
}

function getMessageType(msg) {
  if (msg?.text) return 'text';
  if (msg?.photo) return 'photo';
  if (msg?.video) return 'video';
  if (msg?.document) return 'document';
  if (msg?.audio) return 'audio';
  if (msg?.voice) return 'voice';
  if (msg?.animation) return 'animation';
  if (msg?.sticker) return 'sticker';
  if (msg?.video_note) return 'video_note';
  return 'other';
}

function parseFilters(value) {
  try {
    const parsed = typeof value === 'string' ? JSON.parse(value || '{}') : (value || {});
    return { ...DEFAULT_FILTERS, ...parsed };
  } catch {
    return { ...DEFAULT_FILTERS };
  }
}

function filterText(filters) {
  const labels = {
    text: '文本',
    photo: '图片',
    video: '视频',
    document: '文件',
    audio: '音频',
    voice: '语音',
    animation: '动图',
    sticker: '贴纸',
    video_note: '视频消息',
    other: '其他'
  };
  return Object.keys(labels).map(k => `${filters[k] ? '✅' : '❌'}${labels[k]}`).join('  ');
}

async function getTasks() {
  const p = await db();
  const [rows] = await p.query(
    'SELECT * FROM forward_tasks WHERE admin_id=? ORDER BY id DESC',
    [adminId]
  );
  return rows;
}

async function isAlreadyForwarded(taskId, sourceMessageId) {
  const p = await db();
  const [rows] = await p.query(
    'SELECT id FROM forwarded_messages WHERE task_id=? AND source_message_id=? LIMIT 1',
    [taskId, sourceMessageId]
  );
  return rows.length > 0;
}

async function markForwarded(taskId, sourceMessageId, targetMessageId) {
  const p = await db();
  await p.query(
    `INSERT INTO forwarded_messages (task_id, source_message_id, target_message_id)
     VALUES (?, ?, ?)
     ON DUPLICATE KEY UPDATE target_message_id=VALUES(target_message_id)`,
    [taskId, sourceMessageId, targetMessageId || null]
  );
}

async function syncTask(task) {
  if (runningJobs.has(Number(task.id))) return;
  runningJobs.add(Number(task.id));

  try {
    const p = await db();
    let nextId = Number(task.history_next_id || 0);
    const endId = Number(task.history_end_id || 0);

    if (!nextId || !endId || nextId > endId) {
      await p.query('UPDATE forward_tasks SET status="paused" WHERE id=?', [task.id]);
      return;
    }

    while (nextId <= endId) {
      const [state] = await p.query(
        'SELECT status, history_next_id FROM forward_tasks WHERE id=?',
        [task.id]
      );
      if (!state.length || state[0].status !== 'running') return;

      const batchEnd = Math.min(nextId + 9, endId);

      for (let id = nextId; id <= batchEnd; id++) {
        const [current] = await p.query(
          'SELECT status FROM forward_tasks WHERE id=?',
          [task.id]
        );
        if (!current.length || current[0].status !== 'running') return;

        if (await isAlreadyForwarded(task.id, id)) {
          await p.query(
            'UPDATE forward_tasks SET history_processed=history_processed+1, history_next_id=? WHERE id=?',
            [id + 1, task.id]
          );
          nextId = id + 1;
          continue;
        }

        try {
          const result = await copyWithRetry(task.source_chat_id, task.target_chat_id, id);
          await markForwarded(task.id, id, result?.message_id || null);
          await p.query(
            'UPDATE forward_tasks SET history_processed=history_processed+1, history_next_id=? WHERE id=?',
            [id + 1, task.id]
          );
        } catch (err) {
          if (isNotFoundMessage(err)) {
            await p.query(
              'UPDATE forward_tasks SET history_processed=history_processed+1, history_skipped=history_skipped+1, history_next_id=? WHERE id=?',
              [id + 1, task.id]
            );
            nextId = id + 1;
            continue;
          }

          console.error('历史同步失败', task.id, id, err?.message || err);
          await p.query(
            'UPDATE forward_tasks SET history_failed=history_failed+1,status="paused" WHERE id=?',
            [task.id]
          );
          return;
        }
        nextId = id + 1;
      }
    }

    await p.query(
      'UPDATE forward_tasks SET history_done=1,status="paused",history_next_id=history_end_id+1 WHERE id=?',
      [task.id]
    );
  } finally {
    runningJobs.delete(Number(task.id));
  }
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
  if (!rows.length) return ctx.reply('📋 目前没有转发任务。', menu());

  const lines = rows.map(t => {
    const total = Number(t.history_total || 0);
    const done = Number(t.history_processed || 0);
    const percent = total ? Math.min(100, Math.floor(done * 100 / total)) : 0;
    return [
      `#${t.id}`,
      `源：${t.source_chat_id}`,
      `目标：${t.target_chat_id}`,
      `状态：${t.status}`,
      `历史：${done}/${total}（${percent}%）`,
      `跳过：${t.history_skipped || 0}  失败：${t.history_failed || 0}`,
      `实时：${t.realtime ? '开启' : '关闭'}`
    ].join('\n');
  });
  return ctx.reply('📋 转发任务\n\n' + lines.join('\n\n'), menu());
}

bot.start(async ctx => {
  if (!isAdmin(ctx)) return ctx.reply('机器人已运行。');
  return ctx.reply('🤖 转发机器人\n\n请选择操作：', menu());
});

bot.command('menu', async ctx => {
  if (!isAdmin(ctx)) return;
  return ctx.reply('🤖 主菜单', menu());
});

bot.action('add_task', async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery();
  sessions.set(ctx.from.id, { step: 'source' });
  await ctx.answerCbQuery();
  return ctx.reply('➕ 添加转发任务\n\n请发送【源频道/群】的 ID、@用户名或 t.me 链接。');
});

bot.action('start_sync', async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery();
  const p = await db();
  const [tasks] = await p.query(
    'SELECT * FROM forward_tasks WHERE admin_id=? AND source_chat_id<>target_chat_id AND history_done=0 AND status<>"running"',
    [adminId]
  );

  let started = 0;
  for (const task of tasks) {
    if (!Number(task.history_end_id) || !Number(task.history_next_id)) continue;
    await p.query(
      'UPDATE forward_tasks SET status="running" WHERE id=?',
      [task.id]
    );
    started++;
  }

  await startHistoryJobs();
  await ctx.answerCbQuery('已开始');
  return ctx.reply(
    started
      ? `▶️ 已启动 ${started} 个历史同步任务。\n按每批 10 条处理，重启后会从断点继续。`
      : '⚠️ 没有已设置历史范围的任务。\n请先点击“🕘 设置历史范围”。',
    menu()
  );
});

bot.action('pause_sync', async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery();
  const p = await db();
  await p.query('UPDATE forward_tasks SET status="paused" WHERE admin_id=?', [adminId]);
  await ctx.answerCbQuery('已暂停');
  return ctx.reply('⏸ 所有历史同步任务已暂停。实时转发开关不受影响。', menu());
});

bot.action('realtime', async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery();
  const rows = await getTasks();
  if (!rows.length) return ctx.answerCbQuery('没有任务');
  await ctx.answerCbQuery();
  return ctx.reply(
    '🔄 实时转发\n请选择要设置的任务：',
    Markup.inlineKeyboard(rows.map(t => [
      Markup.button.callback(
        `#${t.id} ${t.source_chat_id} → ${t.target_chat_id}：${t.realtime ? '开启' : '关闭'}`,
        `rt_${t.id}`
      )
    ]))
  );
});

bot.action(/^rt_(\d+)$/, async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery();
  const taskId = Number(ctx.match[1]);
  const p = await db();
  const [rows] = await p.query(
    'SELECT realtime FROM forward_tasks WHERE id=? AND admin_id=?',
    [taskId, adminId]
  );
  if (!rows.length) return ctx.answerCbQuery('任务不存在');

  const enabled = !Number(rows[0].realtime);
  await p.query(
    'UPDATE forward_tasks SET realtime=? WHERE id=? AND admin_id=?',
    [enabled ? 1 : 0, taskId, adminId]
  );
  await ctx.answerCbQuery(enabled ? '已开启' : '已关闭');
  return ctx.reply(
    `🔄 任务 #${taskId} 实时转发已${enabled ? '开启' : '关闭'}。`,
    menu()
  );
});

bot.action('tasks', async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery();
  await ctx.answerCbQuery();
  return showTasks(ctx);
});

bot.action('progress', async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery();
  await ctx.answerCbQuery();
  const rows = await getTasks();
  if (!rows.length) return ctx.reply('📊 暂无任务。', menu());

  const text = rows.map(t => {
    const total = Number(t.history_total || 0);
    const done = Number(t.history_processed || 0);
    const percent = total ? Math.min(100, Math.floor(done * 100 / total)) : 0;
    return `#${t.id}  ${done}/${total}（${percent}%）\n✅ 已处理：${done}  ⏭️ 跳过：${t.history_skipped || 0}  ⚠️ 失败：${t.history_failed || 0}\n状态：${t.status}`;
  }).join('\n\n');

  return ctx.reply('📊 任务进度\n\n' + text, menu());
});

bot.action('filters', async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery();
  const rows = await getTasks();
  if (!rows.length) return ctx.answerCbQuery('没有任务');

  await ctx.answerCbQuery();
  return ctx.reply(
    '🎛 过滤设置\n请选择任务：',
    Markup.inlineKeyboard(rows.map(t => [
      Markup.button.callback(`#${t.id} ${t.source_chat_id} → ${t.target_chat_id}`, `filter_task_${t.id}`)
    ]))
  );
});

bot.action(/^filter_task_(\d+)$/, async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery();
  const taskId = Number(ctx.match[1]);
  const p = await db();
  const [rows] = await p.query('SELECT * FROM forward_tasks WHERE id=? AND admin_id=?', [taskId, adminId]);
  if (!rows.length) return ctx.answerCbQuery('任务不存在');

  const filters = parseFilters(rows[0].filters_json);
  await ctx.answerCbQuery();
  return ctx.reply(
    `🎛 任务 #${taskId} 过滤\n\n${filterText(filters)}`,
    Markup.inlineKeyboard([
      [Markup.button.callback(`文本 ${filters.text ? '✅' : '❌'}`, `ft_${taskId}_text`), Markup.button.callback(`图片 ${filters.photo ? '✅' : '❌'}`, `ft_${taskId}_photo`)],
      [Markup.button.callback(`视频 ${filters.video ? '✅' : '❌'}`, `ft_${taskId}_video`), Markup.button.callback(`文件 ${filters.document ? '✅' : '❌'}`, `ft_${taskId}_document`)],
      [Markup.button.callback(`音频 ${filters.audio ? '✅' : '❌'}`, `ft_${taskId}_audio`), Markup.button.callback(`语音 ${filters.voice ? '✅' : '❌'}`, `ft_${taskId}_voice`)],
      [Markup.button.callback(`动图 ${filters.animation ? '✅' : '❌'}`, `ft_${taskId}_animation`), Markup.button.callback(`贴纸 ${filters.sticker ? '✅' : '❌'}`, `ft_${taskId}_sticker`)],
      [Markup.button.callback(`视频消息 ${filters.video_note ? '✅' : '❌'}`, `ft_${taskId}_video_note`), Markup.button.callback(`其他 ${filters.other ? '✅' : '❌'}`, `ft_${taskId}_other`)],
      [Markup.button.callback('⬅️ 返回', 'filters')]
    ])
  );
});

bot.action(/^ft_(\d+)_(text|photo|video|document|audio|voice|animation|sticker|video_note|other)$/, async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery();
  const taskId = Number(ctx.match[1]);
  const type = ctx.match[2];
  const p = await db();
  const [rows] = await p.query('SELECT filters_json FROM forward_tasks WHERE id=? AND admin_id=?', [taskId, adminId]);
  if (!rows.length) return ctx.answerCbQuery('任务不存在');

  const filters = parseFilters(rows[0].filters_json);
  filters[type] = !filters[type];
  await p.query('UPDATE forward_tasks SET filters_json=? WHERE id=?', [JSON.stringify(filters), taskId]);
  await ctx.answerCbQuery(filters[type] ? '已允许' : '已过滤');
  return ctx.reply(`🎛 任务 #${taskId}\n\n${filterText(filters)}`, menu());
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
  await p.query('DELETE FROM forwarded_messages WHERE task_id=?', [taskId]);
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
  sessions.set(ctx.from.id, { step: 'history_task' });
  await ctx.answerCbQuery();
  return ctx.reply('请发送：任务编号 起始消息ID 结束消息ID，例如：1 100 5000');
});

bot.on('text', async (ctx, next) => {
  if (!isAdmin(ctx)) return next();
  const session = sessions.get(ctx.from.id);
  if (!session) return next();

  if (session.step === 'history_task') {
    const parts = String(ctx.message.text).trim().split(/\s+/);
    const taskId = Number(parts[0]);
    const startId = Number(parts[1]);
    const endId = Number(parts[2]);

    if (!Number.isInteger(taskId) || !Number.isInteger(startId) || !Number.isInteger(endId) || startId < 1 || endId < startId) {
      return ctx.reply('格式错误，请发送：任务编号 起始ID 结束ID，例如：1 100 5000');
    }

    const p = await db();
    const [rows] = await p.query('SELECT * FROM forward_tasks WHERE id=? AND admin_id=?', [taskId, adminId]);
    if (!rows.length) return ctx.reply('❌ 找不到这个任务。');

    const total = endId - startId + 1;
    await p.query(
      `UPDATE forward_tasks
       SET history_next_id=?, history_end_id=?, history_total=?, history_processed=0,
           history_skipped=0, history_failed=0, history_done=0, status="paused"
       WHERE id=?`,
      [startId, endId, total, taskId]
    );
    sessions.delete(ctx.from.id);
    return ctx.reply(`✅ 已设置任务 #${taskId}\n历史范围：${startId} → ${endId}\n总数：${total}\n现在点击“▶️ 开始同步”。`, menu());
  }

  const chat = cleanChatId(ctx.message.text);

  if (session.step === 'source') {
    try {
      const source = await resolveChatId(chat);
      session.source = source;
      session.step = 'target';
      return ctx.reply(`✅ 源已绑定：${source}\n\n现在请发送【目标频道/群】的 ID、@用户名或 t.me 链接。`);
    } catch (err) {
      return ctx.reply(`❌ 无法绑定这个源频道/群。\n\n请确认机器人已经加入该频道/群，并且有读取消息的权限。\n错误：${err?.message || err}`);
    }
  }

  if (session.step === 'target') {
    try {
      const target = await resolveChatId(chat);
      if (Number(target) === Number(session.source)) {
        return ctx.reply('❌ 源和目标不能相同。\n请重新发送目标频道/群。');
      }

      const p = await db();
      await p.query(
        `INSERT INTO forward_tasks
         (admin_id, source_chat_id, target_chat_id, status, realtime, filters_json)
         VALUES (?, ?, ?, 'paused', 1, ?)
         ON DUPLICATE KEY UPDATE updated_at=CURRENT_TIMESTAMP`,
        [adminId, session.source, target, JSON.stringify(DEFAULT_FILTERS)]
      );

      const source = session.source;
      sessions.delete(ctx.from.id);
      return ctx.reply(
        `✅ 转发任务已添加\n\n源：${source}\n目标：${target}\n\n如需历史消息，请先设置历史范围；实时转发默认开启。`,
        menu()
      );
    } catch (err) {
      return ctx.reply(`❌ 无法绑定这个目标频道/群。\n\n请确认机器人已经加入目标频道/群，并且有发送消息的权限。\n错误：${err?.message || err}`);
    }
  }
});

async function handleRealtimeMessage(ctx, message, chatId) {
  if (!message?.message_id || !chatId) return;
  const chatType = ctx.chat?.type;
  if (chatType !== 'group' && chatType !== 'supergroup' && chatType !== 'channel') return;

  const p = await db();
  const [tasks] = await p.query(
    'SELECT * FROM forward_tasks WHERE source_chat_id=? AND realtime=1',
    [chatId]
  );

  const type = getMessageType(message);

  for (const task of tasks) {
    if (Number(task.target_chat_id) === Number(chatId)) continue;

    const filters = parseFilters(task.filters_json);
    if (!filters[type]) continue;

    const lockKey = String(task.id) + ':' + String(message.message_id);
    if (forwardingLocks.has(lockKey)) continue;
    forwardingLocks.add(lockKey);

    try {
      if (await isAlreadyForwarded(task.id, message.message_id)) continue;
      const result = await copyWithRetry(chatId, task.target_chat_id, message.message_id);
      await markForwarded(task.id, message.message_id, result?.message_id || null);
      await p.query(
        'UPDATE forward_tasks SET source_message_id=? WHERE id=?',
        [message.message_id, task.id]
      );
    } catch (err) {
      console.error('实时转发失败', task.id, message.message_id, err?.message || err);
      await p.query(
        'UPDATE forward_tasks SET history_failed=history_failed+1, updated_at=CURRENT_TIMESTAMP WHERE id=?',
        [task.id]
      );
    } finally {
      forwardingLocks.delete(lockKey);
    }
  }
}

bot.on('channel_post', async ctx => {
  try {
    await handleRealtimeMessage(ctx, ctx.channelPost, ctx.chat.id);
  } catch (err) {
    console.error('channel_post handler', err);
  }
});

bot.on('message', async ctx => {
  try {
    const type = ctx.chat?.type;
    if (type !== 'group' && type !== 'supergroup') return;
    await handleRealtimeMessage(ctx, ctx.message, ctx.chat.id);
  } catch (err) {
    console.error('group message handler', err);
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
  await startHistoryJobs();
  console.log('Telegram 转发机器人已启动');
})();

process.once('SIGINT', () => bot.stop('SIGINT'));
process.once('SIGTERM', () => bot.stop('SIGTERM'));
