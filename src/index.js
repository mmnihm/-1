import 'dotenv/config';
import mysql from 'mysql2/promise';
import { Telegraf, Markup } from 'telegraf';
import { TelegramClient } from 'telegram';
import { StringSession } from 'telegram/sessions/index.js';
import { NewMessage } from 'telegram/events/index.js';

const {
  BOT_TOKEN,
  ADMIN_ID,
  MYSQL_HOST,
  MYSQL_PORT = '3306',
  MYSQL_DATABASE,
  MYSQL_USER,
  MYSQL_PASSWORD,
  TG_API_ID,
  TG_API_HASH,
  TG_SESSION = ''
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
const albumQueues = new Map();
const userClients = new Map();
const clientStarting = new Map();

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

    await pool.query(`
      CREATE TABLE IF NOT EXISTS telegram_auth (
        admin_id BIGINT PRIMARY KEY,
        api_id BIGINT NOT NULL,
        api_hash VARCHAR(128) NOT NULL,
        tg_session LONGTEXT NOT NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    `);
  }
  return pool;
}

async function getTelegramAuth(userId) {
  const p = await db();
  const [rows] = await p.query('SELECT api_id, api_hash, tg_session FROM telegram_auth WHERE admin_id=? LIMIT 1',[Number(userId)]);
  return rows[0] || null;
}
async function saveTelegramAuth(userId, apiId, apiHash, tgSession) {
  const p = await db();
  await p.query(`INSERT INTO telegram_auth (admin_id, api_id, api_hash, tg_session)
    VALUES (?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE api_id=VALUES(api_id), api_hash=VALUES(api_hash), tg_session=VALUES(tg_session)`,
    [Number(userId),Number(apiId),apiHash,tgSession]);
}

async function attachTelegramEvents(client, ownerId) {
  const uid=Number(ownerId);
  client.addEventHandler(async event=>{
    try{
      const message=event.message;
      const sourceChatId=message?.chatId!=null?Number(message.chatId):null;
      if(!message?.id||sourceChatId==null)return;
      const p=await db();
      const [tasks]=await p.query(
        'SELECT * FROM forward_tasks WHERE admin_id=? AND source_chat_id=? AND realtime=1',
        [uid,sourceChatId]
      );
      for(const task of tasks){
        if(Number(task.target_chat_id)===sourceChatId)continue;
        const filters=parseFilters(task.filters_json);
        if(!filters[getGramJsMessageType(message)])continue;
        if(message.groupedId!=null){
          const key=`album:${task.id}:${sourceChatId}:${String(message.groupedId)}`;
          let queue=albumQueues.get(key);
          if(!queue){
            queue={task,ownerId:uid,sourceChatId,ids:new Set(),timer:null};
            albumQueues.set(key,queue);
          }
          queue.ids.add(Number(message.id));
          if(queue.timer)clearTimeout(queue.timer);
          queue.timer=setTimeout(()=>forwardTelegramAlbum(key).catch(err=>console.error('MTProto 相册转发失败',task.id,err?.message||err)),700);
        }else{
          await forwardTelegramMessages(task,sourceChatId,[Number(message.id)],uid);
        }
      }
    }catch(err){console.error('MTProto 新消息处理失败',err?.message||err);}
  },new NewMessage({}));
}

async function startTelegramUserClient(userId) {
  const uid=Number(userId);
  const saved=await getTelegramAuth(uid);
  const apiId=saved?.api_id||TG_API_ID;
  const apiHash=saved?.api_hash||TG_API_HASH;
  const tgSession=saved?.tg_session||(uid===adminId?TG_SESSION:'');
  if(!apiId||!apiHash||!tgSession)return null;
  if(clientStarting.has(uid))return clientStarting.get(uid);
  const promise=(async()=>{
    const old=userClients.get(uid);
    if(old){try{await old.disconnect();}catch{}}
    const client=new TelegramClient(new StringSession(tgSession),Number(apiId),apiHash,{connectionRetries:5});
    await client.connect();
    if(!(await client.checkAuthorization()))throw new Error('Telegram 登录会话无效，请重新登录');
    await attachTelegramEvents(client,uid);
    userClients.set(uid,client);
    return client;
  })();
  clientStarting.set(uid,promise);
  try{return await promise;}finally{clientStarting.delete(uid);}
}

async function forwardTelegramAlbum(key) {
  const queue = albumQueues.get(key);
  if (!queue) return;
  albumQueues.delete(key);
  const ids = [...queue.ids].sort((a, b) => a - b);
  if (ids.length) await forwardTelegramMessages(queue.task, queue.sourceChatId, ids, queue.ownerId);
}

async function forwardTelegramMessages(task, sourceChatId, messageIds, ownerId) {
  const client=userClients.get(Number(ownerId));
  if(!client)return;
  const p = await db();
  const pending = [];
  for (const id of [...new Set(messageIds.map(Number).filter(Boolean))]) {
    const lockKey = `mt:${task.id}:${id}`;
    if (forwardingLocks.has(lockKey)) continue;
    forwardingLocks.add(lockKey);
    if (await isAlreadyForwarded(task.id, id)) {
      forwardingLocks.delete(lockKey);
      continue;
    }
    pending.push({ id, lockKey });
  }
  if (!pending.length) return;

  try {
    const target = await client.getEntity(Number(task.target_chat_id));
    const source = await client.getEntity(Number(sourceChatId));
    const result = await client.forwardMessages(target, {
      messages: pending.map(x => x.id),
      fromPeer: source
    });
    const forwarded = Array.isArray(result) ? result : [result];

    for (let i = 0; i < pending.length; i++) {
      await markForwarded(
        task.id,
        pending[i].id,
        Number(forwarded[i]?.id || 0)
      );
    }

    await p.query(
      'UPDATE forward_tasks SET source_message_id=? WHERE id=?',
      [pending[pending.length - 1].id, task.id]
    );
  } catch (err) {
    console.error(
      'MTProto 实时转发失败',
      task.id,
      pending.map(x => x.id).join(','),
      err?.message || err
    );
    await p.query(
      'UPDATE forward_tasks SET history_failed=history_failed+1, updated_at=CURRENT_TIMESTAMP WHERE id=?',
      [task.id]
    );
  } finally {
    for (const item of pending) forwardingLocks.delete(item.lockKey);
  }
}

function getGramJsMessageType(message) {
  if (message?.message) return 'text';
  if (message?.photo) return 'photo';
  if (message?.video) return 'video';
  if (message?.document) return 'document';
  if (message?.audio) return 'audio';
  if (message?.voice) return 'voice';
  if (message?.gif) return 'animation';
  if (message?.sticker) return 'sticker';
  if (message?.videoNote) return 'video_note';
  return 'other';
}

function isAdmin(ctx) {
  return Number(ctx.from?.id) === adminId;
}

function menu(userId) {
  const loggedIn=userClients.has(Number(userId));
  return Markup.inlineKeyboard([
    [Markup.button.callback(loggedIn?'✅ Telegram账号已登录':'🔐 Telegram账号登录','tg_login')],
    [Markup.button.callback('➕ 添加任务','add_task')],
    [Markup.button.callback('🕘 设置历史范围','set_history')],
    [Markup.button.callback('▶️ 开始同步','start_sync'),Markup.button.callback('⏸ 暂停同步','pause_sync')],
    [Markup.button.callback('🔄 实时转发','realtime')],
    [Markup.button.callback('🎛 过滤设置','filters'),Markup.button.callback('📊 任务进度','progress')],
    [Markup.button.callback('📋 我的任务','tasks'),Markup.button.callback('🗑 删除任务','delete_task')],
    ...(loggedIn?[[Markup.button.callback('🔓 退出 Telegram账号','tg_logout')]]:[])
  ]);
}

function cleanChatId(value) {
  const s = String(value || '').trim();
  if (/^-?\d+$/.test(s)) return Number(s);
  const m = s.match(/(?:https?:\/\/)?(?:t\.me\/|@)([A-Za-z0-9_]+)/i);
  return m ? '@' + m[1] : s;
}

async function resolveChatId(value, client) {
  const cleaned=cleanChatId(value);
  if(!cleaned)throw new Error('频道/群不能为空');
  if(typeof cleaned==='number')return cleaned;
  if(!client)throw new Error('请先登录 Telegram');
  const entity=await client.getEntity(cleaned);
  const id=Number(entity?.id);
  if(!Number.isFinite(id))throw new Error('无法获取频道/群 ID');
  if(entity.className==='Channel')return -1000000000000+id;
  if(entity.className==='Chat')return -id;
  return id;
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

async function getTasks(userId) {
  const p=await db();
  const [rows]=await p.query('SELECT * FROM forward_tasks WHERE admin_id=? ORDER BY id DESC',[Number(userId)]);
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
  const taskId=Number(task.id);
  const userId=Number(task.admin_id);
  if(runningJobs.has(taskId))return;
  runningJobs.add(taskId);

  try{
    const client=userClients.get(userId);
    if(!client){
      console.error('历史同步暂停：Telegram账号未登录',taskId,userId);
      return;
    }

    const p=await db();
    let nextId=Number(task.history_next_id||0);
    const endId=Number(task.history_end_id||0);
    if(!nextId||!endId||nextId>endId){
      await p.query('UPDATE forward_tasks SET status="paused" WHERE id=? AND admin_id=?',[taskId,userId]);
      return;
    }

    const source=await client.getEntity(Number(task.source_chat_id));
    const target=await client.getEntity(Number(task.target_chat_id));
    const filters=parseFilters(task.filters_json);

    while(nextId<=endId){
      const [state]=await p.query('SELECT status FROM forward_tasks WHERE id=? AND admin_id=?',[taskId,userId]);
      if(!state.length||state[0].status!=='running')return;

      const batchEnd=Math.min(nextId+9,endId);
      const ids=[];

      for(let id=nextId;id<=batchEnd;id++){
        if(await isAlreadyForwarded(taskId,id)){
          await p.query('UPDATE forward_tasks SET history_processed=history_processed+1,history_skipped=history_skipped+1,history_next_id=? WHERE id=? AND admin_id=?',[id+1,taskId,userId]);
          continue;
        }

        let msg;
        try{
          const got=await client.getMessages(source,{ids:[id]});
          msg=Array.isArray(got)?got[0]:got;
        }catch(err){
          console.error('读取历史消息失败',taskId,id,err?.message||err);
          msg=null;
        }

        if(!msg){
          await p.query('UPDATE forward_tasks SET history_processed=history_processed+1,history_skipped=history_skipped+1,history_next_id=? WHERE id=? AND admin_id=?',[id+1,taskId,userId]);
          continue;
        }

        if(!filters[getGramJsMessageType(msg)]){
          await p.query('UPDATE forward_tasks SET history_processed=history_processed+1,history_skipped=history_skipped+1,history_next_id=? WHERE id=? AND admin_id=?',[id+1,taskId,userId]);
          continue;
        }

        ids.push(id);
      }

      if(ids.length){
        try{
          const out=await client.forwardMessages(target,{messages:ids,fromPeer:source});
          const arr=Array.isArray(out)?out:[out];

          for(let i=0;i<ids.length;i++){
            await markForwarded(taskId,ids[i],Number(arr[i]?.id||0));
            await p.query('UPDATE forward_tasks SET history_processed=history_processed+1,history_next_id=? WHERE id=? AND admin_id=?',[ids[i]+1,taskId,userId]);
          }
        }catch(err){
          console.error('历史批量转发失败，逐条重试',taskId,err?.message||err);

          for(const id of ids){
            try{
              const out=await client.forwardMessages(target,{messages:[id],fromPeer:source});
              const one=Array.isArray(out)?out[0]:out;
              await markForwarded(taskId,id,Number(one?.id||0));
              await p.query('UPDATE forward_tasks SET history_processed=history_processed+1,history_next_id=? WHERE id=? AND admin_id=?',[id+1,taskId,userId]);
            }catch(singleErr){
              console.error('历史单条转发失败',taskId,id,singleErr?.message||singleErr);
              await p.query('UPDATE forward_tasks SET history_processed=history_processed+1,history_failed=history_failed+1,history_next_id=? WHERE id=? AND admin_id=?',[id+1,taskId,userId]);
            }
          }
        }
      }

      nextId=batchEnd+1;
    }

    await p.query('UPDATE forward_tasks SET history_done=1,status="paused",history_next_id=history_end_id+1 WHERE id=? AND admin_id=?',[taskId,userId]);
  }catch(err){
    console.error('历史任务异常',taskId,err?.message||err);
    try{
      const p=await db();
      await p.query('UPDATE forward_tasks SET status="paused",history_failed=history_failed+1 WHERE id=? AND admin_id=?',[taskId,userId]);
    }catch{}
  }finally{
    runningJobs.delete(taskId);
  }
}

function isNotFoundMessage(err) {
  const code = Number(err?.response?.error_code || 0);
  const desc = String(err?.response?.description || err?.message || '');
  return code === 400 && /message to copy not found|message_id_invalid|message not found/i.test(desc);
}

async function startHistoryJobs() {
  const p=await db();
  const [tasks]=await p.query('SELECT * FROM forward_tasks WHERE status="running" AND history_done=0 ORDER BY id ASC');
  for(const task of tasks) syncTask(task).catch(err=>console.error('历史任务异常',task.id,err?.message||err));
}

async function showTasks(ctx) {
  const rows = await getTasks(ctx.from.id);
  if (!rows.length) return ctx.reply('📋 目前没有转发任务。', menu(ctx.from.id));

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
  return ctx.reply('📋 转发任务\n\n' + lines.join('\n\n'), menu(ctx.from.id));
}

bot.start(async ctx=>ctx.reply('🤖 Telegram 转发机器人\\n\\n每个用户独立登录自己的 Telegram 账号。\\n登录后可自行设置源频道、目标频道和同步任务。',menu(ctx.from.id)));
bot.command('menu',async ctx=>ctx.reply('🤖 主菜单',menu(ctx.from.id)));

bot.action('tg_login',async ctx=>{
  const uid=Number(ctx.from.id);
  await ctx.answerCbQuery();
  if(userClients.has(uid))return ctx.reply('✅ 你的 Telegram 账号已经登录。\\n\\n可以直接添加任务。',menu(uid));
  if(!TG_API_ID||!TG_API_HASH)return ctx.reply('❌ 服务器尚未配置 TG_API_ID / TG_API_HASH。\\n\\n普通用户不需要填写 API ID/API Hash，请管理员在 VPS 的 .env 中配置一次。');
  sessions.set(uid,{step:'tg_phone'});
  return ctx.reply('🔐 Telegram账号登录\\n\\n普通用户无需填写 API ID 和 API Hash。\\n请输入你自己的 Telegram 手机号（含国家区号，例如 +8613812345678）。');
});

bot.action('tg_logout',async ctx=>{
  const uid=Number(ctx.from.id),p=await db();
  await p.query('DELETE FROM telegram_auth WHERE admin_id=?',[uid]);
  const client=userClients.get(uid);
  if(client){try{await client.disconnect();}catch{}}
  userClients.delete(uid);sessions.delete(uid);
  await ctx.answerCbQuery('已退出');
  return ctx.reply('🔓 你的 Telegram 账号已退出。任务记录不会删除。',menu(uid));
});

bot.action('add_task',async ctx=>{
  const uid=Number(ctx.from.id);
  if(!userClients.has(uid)){
    await ctx.answerCbQuery('请先登录 Telegram');
    return ctx.reply('❌ 请先点击“🔐 Telegram账号登录”，登录你自己的 Telegram 账号。',menu(uid));
  }
  sessions.set(uid,{step:'source'});
  await ctx.answerCbQuery();
  return ctx.reply('➕ 添加转发任务\\n\\n第1步：发送【源频道/群】的 ID、@用户名或 t.me 链接。\\n第2步：再发送【目标频道/群】。');
});

bot.action('start_sync', async ctx => {
  const uid = Number(ctx.from.id);
  if (!userClients.has(uid)) {
    await ctx.answerCbQuery('请先登录 Telegram');
    return ctx.reply('❌ 请先登录你的 Telegram 账号。', menu(uid));
  }
  const p = await db();
  const [tasks] = await p.query(
    'SELECT * FROM forward_tasks WHERE admin_id=? AND source_chat_id<>target_chat_id AND history_done=0 AND status<>"running"',
    [uid]
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
    menu(uid)
  );
});

bot.action('pause_sync', async ctx => {
  const uid = Number(ctx.from.id);
  const p = await db();
  await p.query('UPDATE forward_tasks SET status="paused" WHERE admin_id=?', [uid]);
  await ctx.answerCbQuery('已暂停');
  return ctx.reply('⏸ 你的所有历史同步任务已暂停。实时转发开关不受影响。', menu(uid));
});

bot.action('realtime', async ctx => {
  const rows = await getTasks(ctx.from.id);
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
  const taskId = Number(ctx.match[1]);
  const p = await db();
  const [rows] = await p.query(
    'SELECT realtime FROM forward_tasks WHERE id=? AND admin_id=?',
    [taskId, Number(ctx.from.id)]
  );
  if (!rows.length) return ctx.answerCbQuery('任务不存在');

  const enabled = !Number(rows[0].realtime);
  await p.query(
    'UPDATE forward_tasks SET realtime=? WHERE id=? AND admin_id=?',
    [enabled ? 1 : 0, taskId, Number(ctx.from.id)]
  );
  await ctx.answerCbQuery(enabled ? '已开启' : '已关闭');
  return ctx.reply(
    `🔄 任务 #${taskId} 实时转发已${enabled ? '开启' : '关闭'}。`,
    menu()
  );
});

bot.action('tasks', async ctx => {
  await ctx.answerCbQuery();
  return showTasks(ctx);
});

bot.action('progress', async ctx => {
  await ctx.answerCbQuery();
  const rows = await getTasks(ctx.from.id);
  if (!rows.length) return ctx.reply('📊 暂无任务。', menu(ctx.from.id));

  const text = rows.map(t => {
    const total = Number(t.history_total || 0);
    const done = Number(t.history_processed || 0);
    const percent = total ? Math.min(100, Math.floor(done * 100 / total)) : 0;
    return `#${t.id}  ${done}/${total}（${percent}%）\n✅ 已处理：${done}  ⏭️ 跳过：${t.history_skipped || 0}  ⚠️ 失败：${t.history_failed || 0}\n状态：${t.status}`;
  }).join('\n\n');

  return ctx.reply('📊 任务进度\n\n' + text, menu(ctx.from.id));
});

bot.action('filters', async ctx => {
  const rows = await getTasks(ctx.from.id);
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
  const taskId = Number(ctx.match[1]);
  const p = await db();
  const [rows] = await p.query('SELECT * FROM forward_tasks WHERE id=? AND admin_id=?', [taskId, Number(ctx.from.id)]);
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
  const taskId = Number(ctx.match[1]);
  const type = ctx.match[2];
  const p = await db();
  const [rows] = await p.query('SELECT filters_json FROM forward_tasks WHERE id=? AND admin_id=?', [taskId, Number(ctx.from.id)]);
  if (!rows.length) return ctx.answerCbQuery('任务不存在');

  const filters = parseFilters(rows[0].filters_json);
  filters[type] = !filters[type];
  await p.query('UPDATE forward_tasks SET filters_json=? WHERE id=?', [JSON.stringify(filters), taskId]);
  await ctx.answerCbQuery(filters[type] ? '已允许' : '已过滤');
  return ctx.reply(`🎛 任务 #${taskId}\n\n${filterText(filters)}`, menu(ctx.from.id));
});

bot.action('delete_task', async ctx => {
  const rows = await getTasks(ctx.from.id);
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
  const taskId = Number(ctx.match[1]);
  const p = await db();
  await p.query('DELETE FROM forwarded_messages WHERE task_id=?', [taskId]);
  const [r] = await p.query(
    'DELETE FROM forward_tasks WHERE id=? AND admin_id=?',
    [taskId, Number(ctx.from.id)]
  );
  await ctx.answerCbQuery(r.affectedRows ? '已删除' : '任务不存在');
  return ctx.reply(r.affectedRows ? `🗑 任务 #${taskId} 已删除。` : '⚠️ 任务不存在。', menu(ctx.from.id));
});

bot.action('set_history', async ctx => {
  const rows = await getTasks(ctx.from.id);
  if (!rows.length) return ctx.answerCbQuery('没有任务');
  sessions.set(ctx.from.id, { step: 'history_task' });
  await ctx.answerCbQuery();
  return ctx.reply('请发送：任务编号 起始消息ID 结束消息ID，例如：1 100 5000');
});

bot.on('text', async (ctx, next) => {
  const uid=Number(ctx.from.id);
  const session = sessions.get(uid);
  if (!session) return next();

  if(session.step==='tg_phone'){
    const phone=String(ctx.message.text).trim();
    if(!/^\\+?[1-9]\\d{6,14}$/.test(phone))return ctx.reply('❌ 手机号格式不正确，请重新发送。');
    session.phone=phone;
    runTelegramBotLogin(uid).catch(err=>{
      console.error('用户 Telegram 登录失败',uid,err?.message||err);
      sessions.delete(uid);
      bot.telegram.sendMessage(uid,`❌ Telegram 登录失败：${err?.message||err}\\n\\n请重新点击“🔐 Telegram账号登录”。`,menu(uid)).catch(()=>{});
    });
    return ctx.reply('⏳ 正在请求 Telegram 验证码，请稍候……');
  }
  if(session.step==='tg_code'){
    const resolve=session.codeResolve;session.codeResolve=null;if(resolve)resolve(String(ctx.message.text).trim());return;
  }
  if(session.step==='tg_password'){
    const resolve=session.passwordResolve;session.passwordResolve=null;if(resolve)resolve(String(ctx.message.text));return;
  }

  if (session.step === 'history_task') {
    const parts = String(ctx.message.text).trim().split(/\s+/);
    const taskId = Number(parts[0]);
    const startId = Number(parts[1]);
    const endId = Number(parts[2]);

    if (!Number.isInteger(taskId) || !Number.isInteger(startId) || !Number.isInteger(endId) || startId < 1 || endId < startId) {
      return ctx.reply('格式错误，请发送：任务编号 起始ID 结束ID，例如：1 100 5000');
    }

    const p = await db();
    const [rows] = await p.query('SELECT * FROM forward_tasks WHERE id=? AND admin_id=?', [taskId, Number(ctx.from.id)]);
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
    return ctx.reply(`✅ 已设置任务 #${taskId}\n历史范围：${startId} → ${endId}\n总数：${total}\n现在点击“▶️ 开始同步”。`, menu(ctx.from.id));
  }

  const chat = cleanChatId(ctx.message.text);

  if (session.step === 'source') {
    try {
      const source = await resolveChatId(chat,userClients.get(uid));
      session.source = source;
      session.step = 'target';
      return ctx.reply(`✅ 源已绑定：${source}\n\n现在请发送【目标频道/群】的 ID、@用户名或 t.me 链接。`);
    } catch (err) {
      return ctx.reply(`❌ 无法绑定这个源频道/群。\n\n请确认机器人已经加入该频道/群，并且有读取消息的权限。\n错误：${err?.message || err}`);
    }
  }

  if (session.step === 'target') {
    try {
      const target = await resolveChatId(chat,userClients.get(uid));
      if (Number(target) === Number(session.source)) {
        return ctx.reply('❌ 源和目标不能相同。\n请重新发送目标频道/群。');
      }

      const p = await db();
      await p.query(
        `INSERT INTO forward_tasks
         (admin_id, source_chat_id, target_chat_id, status, realtime, filters_json)
         VALUES (?, ?, ?, 'paused', 1, ?)
         ON DUPLICATE KEY UPDATE updated_at=CURRENT_TIMESTAMP`,
        [uid, session.source, target, JSON.stringify(DEFAULT_FILTERS)]
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

async function waitTelegramInput(userId,step,field,prompt){
  const session=sessions.get(Number(userId));
  if(!session)throw new Error('登录会话已结束');
  session.step=step;
  await bot.telegram.sendMessage(Number(userId),prompt);
  return await new Promise((resolve,reject)=>{session[field]=resolve;session.rejectInput=reject;});
}

async function runTelegramBotLogin(userId){
  const uid=Number(userId),session=sessions.get(uid);
  if(!session?.phone)throw new Error('登录会话不存在');
  if(!TG_API_ID||!TG_API_HASH)throw new Error('服务器没有配置 TG_API_ID / TG_API_HASH');

  const client=new TelegramClient(new StringSession(''),Number(TG_API_ID),TG_API_HASH,{connectionRetries:5});
  await client.start({
    phoneNumber:async()=>session.phone,
    phoneCode:async()=>waitTelegramInput(uid,'tg_code','codeResolve','📲 Telegram 验证码已发送，请把验证码发送给我。'),
    password:async()=>waitTelegramInput(uid,'tg_password','passwordResolve','🔐 请输入 Telegram 两步验证密码。'),
    onError:err=>console.error('Telegram 登录错误',uid,err?.message||err)
  });
  await saveTelegramAuth(uid,TG_API_ID,TG_API_HASH,client.session.save());
  const old=userClients.get(uid);if(old){try{await old.disconnect();}catch{}}
  await attachTelegramEvents(client,uid);
  userClients.set(uid,client);sessions.delete(uid);
  return bot.telegram.sendMessage(uid,'✅ Telegram账号登录成功！\\n\\n以后你的任务都会使用这个 Telegram 账号执行。\\nVPS 重启后会自动恢复登录状态。',menu(uid));
}

async function restoreAllTelegramClients(){
  const p=await db();
  const [rows]=await p.query('SELECT admin_id,api_id,api_hash,tg_session FROM telegram_auth');
  for(const row of rows){
    try{await startTelegramUserClient(Number(row.admin_id));console.log(`Telegram账号已恢复：用户 ${row.admin_id}`);}
    catch(err){console.error(`Telegram账号恢复失败：用户 ${row.admin_id}`,err?.message||err);}
  }
  if(adminId>0&&TG_SESSION&&TG_API_ID&&TG_API_HASH&&!userClients.has(adminId)){
    try{await saveTelegramAuth(adminId,TG_API_ID,TG_API_HASH,TG_SESSION);await startTelegramUserClient(adminId);}
    catch(err){console.error('恢复管理员环境变量 Telegram 会话失败',err?.message||err);}
  }
}

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
  await restoreAllTelegramClients();
  await bot.launch();
  await startHistoryJobs();
  console.log('Telegram 转发机器人已启动（多用户模式）');
})();

process.once('SIGINT', async () => {
  for (const client of userClients.values()) {
    try { await client.disconnect(); } catch {}
  }
  bot.stop('SIGINT');
});
process.once('SIGTERM', async () => {
  for (const client of userClients.values()) {
    try { await client.disconnect(); } catch {}
  }
  bot.stop('SIGTERM');
});
