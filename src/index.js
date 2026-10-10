import 'dotenv/config';
import mysql from 'mysql2/promise';
import { Telegraf, Markup } from 'telegraf';
import { TelegramClient, Api } from 'telegram';
import { CustomFile } from 'telegram/client/uploads.js';
import { StringSession } from 'telegram/sessions/index.js';
import { NewMessage } from 'telegram/events/index.js';
import QRCode from 'qrcode';

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
const botUserNotifications = new Set();
const topicCloneLocks = new Map();
const discussionRepairJobs = new Set();
const discussionAlbumQueues = new Map();

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
  other: true,
  block_links: false,
  remove_links: false,
  block_keywords: [],
  remove_keywords: [],
  replace_rules: []
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
      'ALTER TABLE forward_tasks ADD COLUMN filters_json TEXT NULL',
      'ALTER TABLE forward_tasks ADD COLUMN history_start_date DATETIME NULL',
      'ALTER TABLE forward_tasks ADD COLUMN history_end_date DATETIME NULL'
    ];
    for (const sql of migrations) {
      try { await pool.query(sql); }
      catch (e) {
        if (e?.code !== 'ER_DUP_FIELDNAME') throw e;
      }
    }

    await pool.query(`
      CREATE TABLE IF NOT EXISTS forward_rate_limits (
        admin_id BIGINT NOT NULL,
        day_key VARCHAR(10) NOT NULL,
        forwarded_count INT NOT NULL DEFAULT 0,
        next_allowed_at DATETIME NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (admin_id, day_key)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    `);

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

    await pool.query(`
      CREATE TABLE IF NOT EXISTS bot_users (
        user_id BIGINT PRIMARY KEY,
        status VARCHAR(20) NOT NULL DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS telegram_topic_maps (
        id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        task_id BIGINT UNSIGNED NOT NULL,
        source_topic_id BIGINT NOT NULL,
        target_topic_id BIGINT NOT NULL,
        title VARCHAR(128) NOT NULL,
        icon_color INT DEFAULT NULL,
        icon_emoji_id VARCHAR(64) DEFAULT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uq_topic_map (task_id, source_topic_id),
        KEY idx_topic_target (task_id, target_topic_id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    `);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS telegram_discussion_maps (
        id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        task_id BIGINT UNSIGNED NOT NULL,
        source_post_id BIGINT NOT NULL,
        source_discussion_chat_id BIGINT NOT NULL,
        source_discussion_root_id BIGINT NOT NULL,
        target_post_id BIGINT NOT NULL,
        target_discussion_chat_id BIGINT NOT NULL,
        target_discussion_root_id BIGINT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uq_discussion_post (task_id, source_post_id),
        UNIQUE KEY uq_discussion_root (task_id, source_discussion_chat_id, source_discussion_root_id),
        KEY idx_discussion_chat (task_id, source_discussion_chat_id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    `);

    await pool.query(`
      INSERT IGNORE INTO bot_users (user_id, status)
      SELECT admin_id, 'authorized' FROM telegram_auth
    `);
    await pool.query(`
      INSERT IGNORE INTO bot_users (user_id, status)
      SELECT admin_id, 'authorized' FROM forward_tasks
    `);
    await pool.query(
      `INSERT INTO bot_users (user_id, status) VALUES (?, 'authorized')
       ON DUPLICATE KEY UPDATE status=IF(status='disabled', status, 'authorized')`,
      [adminId]
    );
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
      const sourceChatId = message?.peerId
        ? Number(await client.getPeerId(message.peerId))
        : (message?.chatId!=null ? Number(message.chatId) : null);
      if(!message?.id||sourceChatId==null)return;
      const p=await db();

      // 先处理频道关联 Discussion 评论区：评论实际发生在关联讨论群。
      const [discussionTasks]=await p.query(
        'SELECT DISTINCT t.* FROM forward_tasks t INNER JOIN telegram_discussion_maps d ON d.task_id=t.id WHERE t.admin_id=? AND t.realtime=1 AND d.source_discussion_chat_id=?',
        [uid,sourceChatId]
      );
      for(const task of discussionTasks){
        try {
          await forwardDiscussionRealtime(task,sourceChatId,Number(message.id),uid);
        } catch(err) {
          console.error('MTProto 评论区实时转发失败',task.id,message.id,err?.message||err);
        }
      }

      // 再处理正常源频道/群消息。
      const [tasks]=await p.query(
        'SELECT * FROM forward_tasks WHERE admin_id=? AND source_chat_id=? AND realtime=1',
        [uid,sourceChatId]
      );
      for(const task of tasks){
        if(Number(task.target_chat_id)===sourceChatId)continue;
        const filters=parseFilters(task.filters_json);
        if(message.groupedId==null && !shouldForwardMessage(message,filters))continue;
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

async function repairLegacyTaskChatIds(userId) {
  const uid=Number(userId);
  const p=await db();
  const [rows]=await p.query(
    'SELECT id,source_chat_id,target_chat_id FROM forward_tasks WHERE admin_id=?',
    [uid]
  );
  for(const row of rows){
    const convert=(value)=>{
      const n=Number(value);
      if(!Number.isSafeInteger(n))return n;
      // 旧版本错误生成：-1000000000000 + 原频道 ID
      // 这类值通常以 -99... 开头；正确格式应为 -100 + 原频道 ID。
      if(n < -900000000000 && n > -1000000000000){
        const raw=n+1000000000000;
        if(raw>0)return Number('-100'+String(raw));
      }
      return n;
    };
    const source=convert(row.source_chat_id);
    const target=convert(row.target_chat_id);
    if(source!==Number(row.source_chat_id)||target!==Number(row.target_chat_id)){
      await p.query(
        'UPDATE forward_tasks SET source_chat_id=?,target_chat_id=?,updated_at=CURRENT_TIMESTAMP WHERE id=? AND admin_id=?',
        [source,target,Number(row.id),uid]
      );
      console.log('已修复旧版频道 ID',Number(row.id),row.source_chat_id,'→',source,row.target_chat_id,'→',target);
    }
  }
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
    await repairLegacyTaskChatIds(uid);
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
  const client = userClients.get(Number(queue.ownerId));
  const ids = new Set([...queue.ids].map(Number));
  if (client) {
    try {
      const source = await client.getEntity(Number(queue.sourceChatId));
      for (const id of [...ids]) {
        const got = await client.getMessages(source, { ids: [id] });
        const msg = Array.isArray(got) ? got[0] : got;
        if (!msg?.groupedId) continue;
        const around = await client.getMessages(source, { limit: 30, around: Number(msg.id) });
        for (const item of around || []) {
          if (item?.groupedId != null && String(item.groupedId) === String(msg.groupedId)) ids.add(Number(item.id));
        }
      }
    } catch (err) {
      console.error('读取完整相册失败，将暂停本组避免拆散转发', queue.task.id, err?.message || err);
      return;
    }
  }
  if (ids.size) await forwardTelegramMessages(queue.task, queue.sourceChatId, [...ids].sort((a, b) => a - b), queue.ownerId);
}

function getForumTopicId(message) {
  const reply = message?.replyTo;
  const top = Number(reply?.replyToTopId || 0);
  if (top > 0) return top;

  const replyToMsgId = Number(reply?.replyToMsgId || 0);
  const isForumReply = Boolean(
    reply?.forumTopic ||
    reply?.className === 'MessageReplyHeader' && reply?.forumTopic ||
    reply?.constructor?.name === 'MessageReplyHeader' && reply?.forumTopic
  );
  if (isForumReply && replyToMsgId > 0) return replyToMsgId;

  const actionName = message?.action?.className || message?.action?.constructor?.name || '';
  if (/MessageActionTopicCreate/i.test(actionName) && Number(message?.id) > 0) {
    return Number(message.id);
  }

  // 某些频道话题的首条普通消息没有 TopicCreate action，但 Telegram 会标记 forumTopic。
  // 此时 replyToMsgId 是该话题的根消息 ID；不再把普通消息 ID 猜成话题 ID。
  if (reply?.forumTopic && replyToMsgId > 0) return replyToMsgId;
  return 0;
}

async function getForumTopicInfo(client, sourceEntity, topicId) {
  const id = Number(topicId || 0);
  if (!id) return null;
  if (id === 1) return { id: 1, title: 'General', iconColor: 0x6FB9F0, iconEmojiId: null };
  try {
    const result = await client.invoke(new Api.channels.GetForumTopicsByID({
      channel: sourceEntity,
      topics: [id]
    }));
    const topic = result?.topics?.find(item => Number(item?.id) === id) || result?.topics?.[0];
    if (!topic) return null;
    return {
      id,
      title: String(topic.title || ('Topic ' + id)).slice(0, 128),
      iconColor: Number(topic.iconColor || 0x6FB9F0),
      iconEmojiId: topic.iconEmojiId != null ? String(topic.iconEmojiId) : null
    };
  } catch (err) {
    console.error('读取话题信息失败', id, err?.message || err);
    return null;
  }
}

async function ensureTargetForumTopic(client, task, sourceEntity, targetEntity, sourceTopicId) {
  const sourceId = Number(sourceTopicId || 0);
  if (!sourceId) return 0;
  if (targetEntity?.forum !== true) return 0;

  const key = `topic:${Number(task.id)}:${sourceId}`;
  if (topicCloneLocks.has(key)) return topicCloneLocks.get(key);

  const promise = (async () => {
    const p = await db();
    const [existing] = await p.query(
      'SELECT target_topic_id FROM telegram_topic_maps WHERE task_id=? AND source_topic_id=? LIMIT 1',
      [Number(task.id), sourceId]
    );
    if (existing.length) return Number(existing[0].target_topic_id);

    if (sourceId === 1) {
      await p.query(
        'INSERT IGNORE INTO telegram_topic_maps (task_id,source_topic_id,target_topic_id,title,icon_color,icon_emoji_id) VALUES (?,?,?,?,?,?)',
        [Number(task.id),1,1,'General',0x6FB9F0,null]
      );
      return 1;
    }

    const info = await getForumTopicInfo(client, sourceEntity, sourceId);
    if (!info) return 0;

    const validColors = new Set([0x6FB9F0,0xFFD67E,0xCB86DB,0x8EEE98,0xFF93B2,0xFB6F5F]);
    const args = {
      channel: targetEntity,
      title: info.title || ('Topic ' + sourceId),
      randomId: BigInt(Date.now()),
      iconColor: validColors.has(info.iconColor) ? info.iconColor : 0x6FB9F0
    };
    if (info.iconEmojiId) args.iconEmojiId = info.iconEmojiId;

    let result;
    try {
      result = await client.invoke(new Api.channels.CreateForumTopic(args));
    } catch (err) {
      if (!info.iconEmojiId) throw err;
      delete args.iconEmojiId;
      result = await client.invoke(new Api.channels.CreateForumTopic(args));
    }

    // GramJS 的 CreateForumTopic 返回 Updates 时，话题创建动作通常位于
    // update.message.action，而不是 update.action；兼容不同 Updates 返回结构。
    const updates = Array.isArray(result?.updates) ? result.updates : [];
    const createdUpdate = updates.find(update => {
      const message = update?.message || update?.msg || null;
      const action = message?.action || update?.action || null;
      return action?.className === 'MessageActionTopicCreate' ||
        action?.constructor?.name === 'MessageActionTopicCreate';
    });
    const createdMessage = createdUpdate?.message || createdUpdate?.msg || null;
    let targetId = Number(createdMessage?.id || createdUpdate?.id || 0);

    // 兜底：部分 GramJS 版本返回的更新对象结构不同，按创建动作再次扫描。
    if (!targetId) {
      for (const update of updates) {
        const candidates = [update?.message, update?.msg, update?.message?.message].filter(Boolean);
        for (const candidate of candidates) {
          const action = candidate?.action || null;
          if (
            (action?.className === 'MessageActionTopicCreate' ||
             action?.constructor?.name === 'MessageActionTopicCreate') &&
            Number(candidate?.id || 0) > 0
          ) {
            targetId = Number(candidate.id);
            break;
          }
        }
        if (targetId) break;
      }
    }

    if (!targetId) {
      const updateSummary = updates.map(update => ({
        type: update?.className || update?.constructor?.name || 'unknown',
        messageId: Number(update?.message?.id || update?.msg?.id || update?.id || 0),
        action: update?.message?.action?.className || update?.msg?.action?.className || update?.action?.className || ''
      }));
      console.error('创建话题返回结果未识别', {
        taskId: Number(task.id),
        sourceTopicId: sourceId,
        title: info.title,
        resultType: result?.className || result?.constructor?.name || typeof result,
        updates: updateSummary
      });
      throw new Error('创建目标话题后未获取到话题 ID（已记录 Telegram 返回结构）');
    }

    await p.query(
      'INSERT IGNORE INTO telegram_topic_maps (task_id,source_topic_id,target_topic_id,title,icon_color,icon_emoji_id) VALUES (?,?,?,?,?,?)',
      [Number(task.id),sourceId,targetId,info.title,info.iconColor,info.iconEmojiId]
    );
    const [saved] = await p.query(
      'SELECT target_topic_id FROM telegram_topic_maps WHERE task_id=? AND source_topic_id=? LIMIT 1',
      [Number(task.id),sourceId]
    );
    return saved.length ? Number(saved[0].target_topic_id) : targetId;
  })();

  topicCloneLocks.set(key, promise);
  try {
    return await promise;
  } finally {
    topicCloneLocks.delete(key);
  }
}

function getFloodWaitSeconds(err) {
  const text = String(err?.message || err || '');
  const fromText = text.match(/FLOOD_WAIT_(\d+)/i) || text.match(/A wait of (\d+) seconds/i);
  const seconds = Number(err?.seconds || err?.retryAfter || fromText?.[1] || 0);
  return Number.isFinite(seconds) && seconds > 0 ? seconds : 0;
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function inferUploadFilename(message) {
  const media = message?.media;
  if (media?.className === 'MessageMediaPhoto') return 'photo.jpg';
  const document = media?.document;
  const attrs = document?.attributes || [];
  for (const attr of attrs) {
    const name = String(attr?.fileName || attr?.file_name || '').trim();
    if (attr?.className === 'DocumentAttributeFilename' && name) return name;
  }
  if (attrs.some(attr => attr?.className === 'DocumentAttributeVideo' || attr?.className === 'DocumentAttributeRoundMessage')) return 'video.mp4';
  if (attrs.some(attr => attr?.className === 'DocumentAttributeAudio')) {
    const audio = attrs.find(attr => attr?.className === 'DocumentAttributeAudio');
    return audio?.voice ? 'voice.ogg' : 'audio.mp3';
  }
  if (attrs.some(attr => attr?.className === 'DocumentAttributeAnimated')) return 'animation.gif';
  if (attrs.some(attr => attr?.className === 'DocumentAttributeSticker')) return 'sticker.webp';
  const mime = String(document?.mimeType || document?.mime_type || '').toLowerCase();
  if (mime === 'image/jpeg') return 'image.jpg';
  if (mime === 'image/png') return 'image.png';
  if (mime === 'image/webp') return 'image.webp';
  if (mime === 'video/mp4') return 'video.mp4';
  if (mime.startsWith('audio/')) return mime === 'audio/ogg' ? 'audio.ogg' : 'audio.mp3';
  return 'file.bin';
}

function makeUploadFile(message, buffer) {
  const data = Buffer.isBuffer(buffer) ? buffer : Buffer.from(buffer);
  return new CustomFile(inferUploadFilename(message), data.length, '', data);
}

const DAILY_FORWARD_LIMIT = 300;
const FORWARD_GROUP_INTERVAL_MS = 15000;

async function reserveForwardSlot(adminId, messageCount) {
  const uid = Number(adminId || 0);
  const count = Math.max(1, Number(messageCount || 1));
  if (!uid) throw new Error('转发限速缺少账号 ID，已停止发送');
  const p = await db();
  const now = new Date();
  const dayKey = now.toISOString().slice(0, 10);
  const conn = await p.getConnection();
  let slotTime = now.getTime();
  try {
    await conn.beginTransaction();
    await conn.query(
      'INSERT IGNORE INTO forward_rate_limits (admin_id, day_key, forwarded_count, next_allowed_at) VALUES (?, ?, 0, NULL)',
      [uid, dayKey]
    );
    const [rows] = await conn.query(
      'SELECT forwarded_count, next_allowed_at FROM forward_rate_limits WHERE admin_id=? AND day_key=? FOR UPDATE',
      [uid, dayKey]
    );
    const used = Number(rows[0]?.forwarded_count || 0);
    if (used + count > DAILY_FORWARD_LIMIT) {
      const err = new Error('已达到账号每日转发上限（' + DAILY_FORWARD_LIMIT + ' 条），任务已暂停，明天可继续');
      err.code = 'DAILY_FORWARD_LIMIT';
      err.dailyLimit = true;
      await conn.rollback();
      throw err;
    }
    const next = rows[0]?.next_allowed_at ? new Date(rows[0].next_allowed_at).getTime() : 0;
    slotTime = Math.max(now.getTime(), Number.isFinite(next) ? next : 0);
    const nextAllowed = new Date(slotTime + FORWARD_GROUP_INTERVAL_MS);
    await conn.query(
      'UPDATE forward_rate_limits SET forwarded_count=forwarded_count+?, next_allowed_at=? WHERE admin_id=? AND day_key=?',
      [count, nextAllowed, uid, dayKey]
    );
    await conn.commit();
  } catch (err) {
    try { await conn.rollback(); } catch {}
    throw err;
  } finally {
    conn.release();
  }
  const waitMs = slotTime - Date.now();
  if (waitMs > 0) await sleep(waitMs);
}

async function sendTelegramMessagesWithoutSource(client, source, target, messages, topicResolver = null, replyResolver = null, filters = null, rateAdminId = 0) {
  // Native forwarding keeps Telegram's original album/media. Use the low-level
  // API because GramJS's forwardMessages helper does not expose topMsgId.
  const list = [...messages].filter(Boolean);
  if (!list.length) return [];
  const sent = [];
  const albumGroups = new Map();
  for (const msg of list) {
    const albumKey = msg.groupedId != null ? 'album:' + String(msg.groupedId) : 'single:' + Number(msg.id);
    if (!albumGroups.has(albumKey)) albumGroups.set(albumKey, []);
    albumGroups.get(albumKey).push(msg);
  }

  for (const album of albumGroups.values()) {
    const items = album.sort((a, b) => Number(a.id) - Number(b.id));
    if (!items.some(item => shouldForwardMessage(item, filters))) continue;
    const first = items[0];
    const topicId = topicResolver ? Number(await topicResolver(first) || 0) : 0;
    const replyId = replyResolver ? Number(await replyResolver(first, topicId) || 0) : 0;
    await reserveForwardSlot(rateAdminId, items.length);

    const sourcePeer = await client.getInputEntity(source);
    const targetPeer = await client.getInputEntity(target);
    const ids = items.map(item => Number(item.id));
    const requestArgs = {
      fromPeer: sourcePeer,
      id: ids,
      toPeer: targetPeer,
      randomId: ids.map((id, index) => BigInt(Date.now()) * 100000n + BigInt(Math.abs(id) * 10 + index + 1)),
      dropAuthor: false
    };
    // For forum groups, topMsgId is the actual target topic ID. replyTo in
    // GramJS's high-level helper is ignored by forwardMessages, causing General-topic posts.
    if (topicId > 1) requestArgs.topMsgId = topicId;
    else if (replyId > 0) requestArgs.topMsgId = replyId;

    const request = new Api.messages.ForwardMessages(requestArgs);
    const result = await client.invoke(request);
    const updateMessages = (Array.isArray(result?.updates) ? result.updates : [])
      .map(update => update?.message || update?.msg || null)
      .filter(message => message && Number(message.id) > 0)
      .sort((a, b) => Number(a.id) - Number(b.id));

    let targetMessages = updateMessages;
    if (updateMessages.length) {
      try {
        const refreshed = await client.getMessages(target, { ids: updateMessages.map(message => Number(message.id)) });
        const byId = new Map((Array.isArray(refreshed) ? refreshed : [refreshed]).filter(Boolean).map(message => [Number(message.id), message]));
        targetMessages = updateMessages.map(message => byId.get(Number(message.id)) || message);
      } catch (refreshError) {
        console.warn('读取刚转发的目标消息失败，使用 Telegram 更新返回值', refreshError?.message || refreshError);
      }
    }

    for (let index = 0; index < items.length; index++) {
      const sourceMessage = items[index];
      const targetMessage = targetMessages[index];
      if (!targetMessage) continue;
      sent.push({ sourceId: Number(sourceMessage.id), sent: targetMessage });

      // Native forwarding preserves the source; apply remove/replace keyword and
      // link rules by editing the newly forwarded message afterwards.
      const originalText = String(sourceMessage.message || '');
      if (originalText && filters && targetMessage.id) {
        const filteredText = applyContentFiltersToText(originalText, filters);
        if (filteredText !== null && filteredText !== originalText) {
          try {
            await client.editMessage(target, { message: Number(targetMessage.id), text: filteredText, linkPreview: false });
            targetMessage.message = filteredText;
          } catch (editError) {
            console.error('转发后编辑关键词/链接失败', sourceMessage.id, targetMessage.id, editError?.message || editError);
          }
        }
      }
    }
  }
  return sent;
}
async function getForwardedTargetMessageId(taskId, sourceMessageId) {
  const p = await db();
  for (let i = 0; i < 4; i++) {
    const [rows] = await p.query(
      'SELECT target_message_id FROM forwarded_messages WHERE task_id=? AND source_message_id=? LIMIT 1',
      [Number(taskId), Number(sourceMessageId)]
    );
    const targetId = Number(rows[0]?.target_message_id || 0);
    if (targetId > 0) return targetId;
    if (i < 3) await new Promise(resolve => setTimeout(resolve, 300));
  }
  return 0;
}

function getDirectReplyMessageId(message, topicId) {
  const reply = message?.replyTo;
  const replyId = Number(reply?.replyToMsgId || 0);
  if (replyId <= 0) return 0;
  if (Number(replyId) === Number(topicId || 0)) return 0;
  if (reply?.forumTopic && Number(reply?.replyToTopId || 0) === replyId) return 0;
  return replyId;
}


async function getDiscussionRoot(client, channelEntity, postId) {
  try {
    const result = await client.invoke(new Api.messages.GetDiscussionMessage({
      peer: channelEntity,
      msgId: Number(postId)
    }));
    // GetDiscussionMessage may return the original channel post before the
    // forwarded root message in the linked discussion group. Never assume
    // messages[0] is the discussion root.
    const sourcePeerId = Number(await client.getPeerId(channelEntity));
    let root = null;
    let discussionChatId = 0;
    for (const candidate of (result?.messages || [])) {
      if (!candidate?.id) continue;
      let candidateChatId = 0;
      try {
        candidateChatId = candidate.peerId
          ? Number(await client.getPeerId(candidate.peerId))
          : Number(candidate.chatId || 0);
      } catch {}
      if (Number.isSafeInteger(candidateChatId) && candidateChatId !== 0 && candidateChatId !== sourcePeerId) {
        root = candidate;
        discussionChatId = candidateChatId;
        break;
      }
    }
    if (!root || !discussionChatId) return null;
    const chat = await client.getEntity(discussionChatId);
    return { root, chat, chatId: discussionChatId };
  } catch (err) {
    if (!/MSG_ID_INVALID|CHANNEL_INVALID|PEER_ID_INVALID/i.test(String(err?.message || ''))) {
      console.error('读取频道评论区失败', postId, err?.message || err);
    }
    return null;
  }
}

async function ensureDiscussionMessageTable() {
  const p = await db();
  await p.query('CREATE TABLE IF NOT EXISTS telegram_discussion_message_maps (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, task_id BIGINT UNSIGNED NOT NULL, source_chat_id BIGINT NOT NULL, source_message_id BIGINT NOT NULL, target_chat_id BIGINT NOT NULL, target_message_id BIGINT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, UNIQUE KEY uq_discussion_message (task_id, source_chat_id, source_message_id), KEY idx_discussion_parent (task_id, source_chat_id, source_message_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4');
}

async function sendDiscussionMessage(client, targetChat, message, replyTo=0) {
  const options = replyTo > 0 ? { replyTo } : {};
  try {
    return await client.sendMessage(targetChat, { message, ...options });
  } catch (firstErr) {
    if (!message?.media) throw firstErr;
    const buffer = await client.downloadMedia(message, {});
    if (!buffer) throw firstErr;
    return await client.sendFile(targetChat, {
      file: makeUploadFile(message, buffer),
      caption: String(message.message || ''),
      forceDocument: false,
      ...options
    });
  }
}

async function sendDiscussionAlbum(client, targetChat, comments, replyTo=0) {
  const media = comments.filter(item => item?.media).sort((a,b) => Number(a.id)-Number(b.id));
  if (!media.length) return [];
  const options = replyTo > 0 ? { replyTo } : {};
  try {
    const result = await client.sendFile(targetChat, {
      file: media.map(item => item.media),
      caption: media.map(item => String(item.message || '')),
      ...options
    });
    return (Array.isArray(result) ? result : [result]).filter(Boolean);
  } catch (firstErr) {
    // 有些 Telegram 媒体对象不能直接复用；下载原媒体后仍以一组发送。
    try {
      const buffers = [];
      for (const item of media) {
        const buffer = await client.downloadMedia(item, {});
        if (!buffer) throw new Error('下载相册媒体失败: ' + item.id);
        buffers.push(buffer);
      }
      const result = await client.sendFile(targetChat, {
        file: media.map((item, index) => makeUploadFile(item, buffers[index])),
        caption: media.map(item => String(item.message || '')),
        forceDocument: false,
        ...options
      });
      return (Array.isArray(result) ? result : [result]).filter(Boolean);
    } catch (secondErr) {
      console.error('评论区相册整组发送失败，改为逐条发送', secondErr?.message || firstErr?.message || firstErr);
      const sent = [];
      for (const item of media) {
        try {
          let result;
          try {
            result = await client.sendMessage(targetChat, { message: item, ...options });
          } catch (sendErr) {
            const buffer = await client.downloadMedia(item, {});
            if (!buffer) throw sendErr;
            result = await client.sendFile(targetChat, {
              file: makeUploadFile(item, buffer),
              caption: String(item.message || ''),
              forceDocument: false,
              ...options
            });
          }
          if (result?.id) sent.push(result);
        } catch (err) {
          console.error('评论区相册单项发送失败', item.id, err?.message || err);
        }
      }
      return sent;
    }
  }
}

async function cloneDiscussionComments(client, task, sourceChannel, sourcePostId, targetPostId) {
  const filters = parseFilters(task.filters_json);
  if (filters.clone_comments === false) return;
  await ensureDiscussionMessageTable();

  const sourceInfo = await getDiscussionRoot(client, sourceChannel, sourcePostId);
  if (!sourceInfo) return;
  const targetChannel = await client.getEntity(Number(task.target_chat_id));
  const targetInfo = await getDiscussionRoot(client, targetChannel, targetPostId);
  if (!targetInfo) {
    console.log('目标频道没有可用评论区，跳过评论同步', task.id, targetPostId);
    return;
  }

  const p = await db();
  await p.query('INSERT INTO telegram_discussion_maps (task_id,source_post_id,source_discussion_chat_id,source_discussion_root_id,target_post_id,target_discussion_chat_id,target_discussion_root_id) VALUES (?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE target_post_id=VALUES(target_post_id),target_discussion_chat_id=VALUES(target_discussion_chat_id),target_discussion_root_id=VALUES(target_discussion_root_id)',
    [Number(task.id),Number(sourcePostId),Number(sourceInfo.chatId),Number(sourceInfo.root.id),Number(targetPostId),Number(targetInfo.chatId),Number(targetInfo.root.id)]);

  let comments = [];
  try {
    for await (const comment of client.iterMessages(sourceInfo.chat, {
      replyTo: Number(sourceInfo.root.id)
    })) {
      if (comment?.id && Number(comment.id) !== Number(sourceInfo.root.id)) {
        comments.push(comment);
      }
    }
    comments.sort((a,b) => Number(a.id)-Number(b.id));
  } catch (err) {
    console.error('读取历史评论失败', task.id, sourcePostId, err?.message || err);
    return;
  }

  for (let index = 0; index < comments.length;) {
    const comment = comments[index];
    const albumId = comment?.groupedId != null ? String(comment.groupedId) : '';
    let batch = [comment];
    index++;

    // 同一 groupedId 的评论媒体必须作为相册整组发送，不能逐条拆开。
    if (albumId) {
      while (index < comments.length && comments[index]?.groupedId != null &&
             String(comments[index].groupedId) === albumId) {
        batch.push(comments[index]);
        index++;
      }
    }

    const eligible = [];
    for (const item of batch) {
      const [exists] = await p.query(
        'SELECT target_message_id FROM telegram_discussion_message_maps WHERE task_id=? AND source_chat_id=? AND source_message_id=? LIMIT 1',
        [Number(task.id), Number(item.chatId ?? sourceInfo.chatId), Number(item.id)]
      );
      if (!exists.length && shouldForwardMessage(item, filters) &&
          applyContentFiltersToMessage(item, filters)) eligible.push(item);
    }
    if (!eligible.length) continue;

    // 若相册里只有部分项目尚未同步，不能把已同步的项目重复发送；
    // 但保留剩余项目的组发送，确保每条源消息都有对应目标消息映射。
    let replyTo = Number(targetInfo.root.id);
    const sourceReplyId = getDirectReplyMessageId(eligible[0], Number(sourceInfo.root.id));
    if (sourceReplyId > 0) {
      const [parent] = await p.query(
        'SELECT target_message_id FROM telegram_discussion_message_maps WHERE task_id=? AND source_chat_id=? AND source_message_id=? LIMIT 1',
        [Number(task.id), Number(eligible[0].chatId ?? sourceInfo.chatId), Number(sourceReplyId)]
      );
      if (parent.length) replyTo = Number(parent[0].target_message_id);
    }

    try {
      const targetChat = targetInfo.chat;
      if (albumId && eligible.filter(item => item.media).length > 1) {
        const sentItems = await sendDiscussionAlbum(client, targetChat, eligible, replyTo);
        for (let j = 0; j < eligible.filter(item => item.media).length; j++) {
          const item = eligible.filter(entry => entry.media)[j];
          const sent = sentItems[j];
          if (!sent?.id) continue;
          await p.query(
            'INSERT IGNORE INTO telegram_discussion_message_maps (task_id,source_chat_id,source_message_id,target_chat_id,target_message_id) VALUES (?,?,?,?,?)',
            [Number(task.id), Number(item.chatId ?? sourceInfo.chatId), Number(item.id), Number(targetInfo.chatId), Number(sent.id)]
          );
        }
      } else {
        for (const item of eligible) {
          const filtered = applyContentFiltersToMessage(item, filters);
          if (!filtered) continue;
          const sent = await sendDiscussionMessage(client, targetChat, filtered, replyTo);
          if (!sent?.id) continue;
          await p.query(
            'INSERT IGNORE INTO telegram_discussion_message_maps (task_id,source_chat_id,source_message_id,target_chat_id,target_message_id) VALUES (?,?,?,?,?)',
            [Number(task.id), Number(item.chatId ?? sourceInfo.chatId), Number(item.id), Number(targetInfo.chatId), Number(sent.id)]
          );
        }
      }
    } catch (err) {
      console.error('同步评论失败', task.id, eligible.map(item => item.id).join(','), err?.message || err);
    }
  }
}


async function repairDiscussionMapsForTask(task, ownerId) {
  const client = userClients.get(Number(ownerId));
  if (!client) throw new Error('请先登录 Telegram 账号');
  const filters = parseFilters(task.filters_json);
  if (filters.clone_comments === false) throw new Error('请先开启“克隆评论区”设置');
  const source = await client.getEntity(Number(task.source_chat_id));
  if (source?.className !== 'Channel') throw new Error('源必须是频道，群组任务不支持频道评论区克隆');
  await client.getEntity(Number(task.target_chat_id));
  await ensureDiscussionMessageTable();
  const p = await db();
  const [rows] = await p.query(
    'SELECT source_message_id,target_message_id FROM forwarded_messages WHERE task_id=? AND target_message_id IS NOT NULL ORDER BY source_message_id ASC',
    [Number(task.id)]
  );
  let checked = 0;
  let errors = 0;
  for (const row of rows) {
    const sourcePostId = Number(row.source_message_id || 0);
    const targetPostId = Number(row.target_message_id || 0);
    if (!sourcePostId || !targetPostId) continue;
    try {
      await cloneDiscussionComments(client, task, source, sourcePostId, targetPostId);
    } catch (err) {
      errors++;
      console.error('补齐已有帖子评论失败', task.id, sourcePostId, err?.message || err);
      const wait = getFloodWaitSeconds(err);
      if (wait > 0 && wait <= 180) await sleep(wait * 1000 + 500);
    }
    checked++;
    if (checked % 50 === 0) {
      console.log('评论区补齐进度', task.id, checked, '/', rows.length, 'errors=', errors);
      await sleep(250);
    } else {
      await sleep(100);
    }
  }
  return { checked, total: rows.length, errors };
}

async function forwardDiscussionRealtime(task, sourceChatId, messageId, ownerId) {
  const client = userClients.get(Number(ownerId));
  if (!client) return;
  await ensureDiscussionMessageTable();

  const p = await db();
  const sourceChat = await client.getEntity(Number(sourceChatId));
  const got = await client.getMessages(sourceChat,{ids:[Number(messageId)]});
  const message = Array.isArray(got) ? got[0] : got;
  if (!message) return;

  const filters = parseFilters(task.filters_json);
  if (filters.clone_comments === false || !shouldForwardMessage(message,filters)) return;

  const reply = message?.replyTo;
  let rootId = Number(reply?.replyToTopId || 0);

  // 普通讨论群的根转发帖本身没有 replyTo，不是评论，应安静跳过。
  // 对于缺少 replyToTopId 的评论，先检查直接回复的消息是否就是已登记的根帖；
  // 若回复的是另一条评论，则沿 reply 链向上查找，最多 12 层，避免把评论发错帖子。
  if (!rootId) {
    let cursor = Number(reply?.replyToMsgId || 0);
    if (!cursor) return;

    const sourceChat = await client.getEntity(Number(sourceChatId));
    const seen = new Set();
    for (let depth = 0; cursor > 0 && depth < 12; depth++) {
      if (seen.has(cursor)) break;
      seen.add(cursor);

      const [knownRoot] = await p.query(
        'SELECT id FROM telegram_discussion_maps WHERE task_id=? AND source_discussion_chat_id=? AND source_discussion_root_id=? LIMIT 1',
        [Number(task.id), Number(sourceChatId), cursor]
      );
      if (knownRoot.length) {
        rootId = cursor;
        break;
      }

      const gotParent = await client.getMessages(sourceChat, { ids: [cursor] });
      const parent = Array.isArray(gotParent) ? gotParent[0] : gotParent;
      if (!parent) break;
      const parentReply = parent.replyTo;
      const next = Number(parentReply?.replyToTopId || parentReply?.replyToMsgId || 0);
      if (!next || next === cursor) break;
      cursor = next;
    }
  }

  if (!rootId) {
    console.warn('评论区回复链无法解析根帖，跳过', task.id, sourceChatId, messageId);
    return;
  }

  // 一个讨论群会承载多个频道帖子的评论，必须按“评论所属根消息”精确匹配。
  const [maps] = await p.query(
    'SELECT * FROM telegram_discussion_maps WHERE task_id=? AND source_discussion_chat_id=? AND source_discussion_root_id=? LIMIT 1',
    [Number(task.id),Number(sourceChatId),rootId]
  );
  if(!maps.length) return;
  const map = maps[0];

  const [exists] = await p.query(
    'SELECT target_message_id FROM telegram_discussion_message_maps WHERE task_id=? AND source_chat_id=? AND source_message_id=? LIMIT 1',
    [Number(task.id),Number(sourceChatId),Number(messageId)]
  );
  if (exists.length) return;

  if (message.groupedId != null) {
    // 实时相册稍作合并等待，再通过历史评论扫描器整组补齐；
    // 数据库映射负责去重，避免每张媒体各发一次。
    const albumKey = `discussion-album:${Number(task.id)}:${Number(map.source_post_id)}`;
    let queued = discussionAlbumQueues.get(albumKey);
    if (!queued) {
      queued = { task, ownerId: Number(ownerId), sourceChatId: Number(sourceChatId), sourcePostId: Number(map.source_post_id), targetPostId: Number(map.target_post_id), timer: null };
      discussionAlbumQueues.set(albumKey, queued);
    }
    if (queued.timer) clearTimeout(queued.timer);
    queued.timer = setTimeout(async () => {
      discussionAlbumQueues.delete(albumKey);
      try {
        const liveClient = userClients.get(Number(queued.ownerId));
        if (!liveClient) return;
        const channel = await liveClient.getEntity(Number(queued.task.source_chat_id));
        await cloneDiscussionComments(liveClient, queued.task, channel, queued.sourcePostId, queued.targetPostId);
      } catch (err) {
        console.error('实时评论相册同步失败', queued.task.id, queued.sourcePostId, err?.message || err);
      }
    }, 900);
    return;
  }

  let replyTo = Number(map.target_discussion_root_id);
  const sourceReplyId = getDirectReplyMessageId(message, Number(map.source_discussion_root_id));
  if (sourceReplyId > 0) {
    const [parent] = await p.query(
      'SELECT target_message_id FROM telegram_discussion_message_maps WHERE task_id=? AND source_chat_id=? AND source_message_id=? LIMIT 1',
      [Number(task.id),Number(sourceChatId),Number(sourceReplyId)]
    );
    if (parent.length) replyTo = Number(parent[0].target_message_id);
  }

  try {
    const targetChat = await client.getEntity(Number(map.target_discussion_chat_id));
    const sent = await sendDiscussionMessage(client,targetChat,applyContentFiltersToMessage(message,parseFilters(task.filters_json)),replyTo);
    if (!sent?.id) return;
    await p.query(
      'INSERT IGNORE INTO telegram_discussion_message_maps (task_id,source_chat_id,source_message_id,target_chat_id,target_message_id) VALUES (?,?,?,?,?)',
      [Number(task.id),Number(sourceChatId),Number(messageId),Number(map.target_discussion_chat_id),Number(sent.id)]
    );
  } catch (err) {
    console.error('实时评论同步失败',task.id,messageId,err?.message||err);
  }
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
    const messages = [];
    for (const item of pending) {
      const got = await client.getMessages(source, { ids: [item.id] });
      const msg = Array.isArray(got) ? got[0] : got;
      if (msg) messages.push(msg);
    }

    const topicEnabled = parseFilters(task.filters_json).clone_topics !== false;
    const result = await sendTelegramMessagesWithoutSource(
      client,
      source,
      target,
      messages,
      topicEnabled ? async msg => ensureTargetForumTopic(client, task, source, target, getForumTopicId(msg)) : null,
      async (msg, topicId) => {
        if (parseFilters(task.filters_json).clone_comments === false) return 0;
        const sourceReplyId = getDirectReplyMessageId(msg, getForumTopicId(msg));
        if (!sourceReplyId) return 0;
        return await getForwardedTargetMessageId(task.id, sourceReplyId);
      },
      parseFilters(task.filters_json),
      ownerId
    );
    const forwardedBySource = new Map(
      (Array.isArray(result) ? result : []).map(item => [Number(item?.sourceId || 0), Number(item?.sent?.id || 0)])
    );

    for (const item of pending) {
      const targetId = forwardedBySource.get(item.id) || 0;
      await markForwarded(task.id, item.id, targetId);
      if (targetId > 0 && parseFilters(task.filters_json).clone_comments !== false && source?.className === 'Channel') {
        try { await cloneDiscussionComments(client, task, source, item.id, targetId); }
        catch (discussionErr) { console.error('实时评论区初始化失败',task.id,item.id,discussionErr?.message||discussionErr); }
      }
    }

    await p.query(
      'UPDATE forward_tasks SET source_message_id=? WHERE id=?',
      [pending[pending.length - 1].id, task.id]
    );
  } catch (err) {
    if (err?.code === 'DAILY_FORWARD_LIMIT') {
      console.warn('实时转发达到每日转发上限，暂停任务', task.id, err.message);
      await p.query('UPDATE forward_tasks SET status="paused" WHERE id=?', [task.id]);
      return;
    }
    const wait = getFloodWaitSeconds(err);
    if (wait > 0) {
      console.error('MTProto 实时转发遇到 Telegram 限流，暂停任务避免继续触发限制', task.id, wait, err?.message || err);
      await p.query('UPDATE forward_tasks SET status="paused" WHERE id=?', [task.id]);
      return;
    }
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
  if (message?.sticker) return 'sticker';
  if (message?.videoNote) return 'video_note';
  if (message?.voice) return 'voice';
  if (message?.gif) return 'animation';
  if (message?.audio) return 'audio';
  if (message?.video) return 'video';
  if (message?.photo) return 'photo';
  if (message?.document) return 'document';
  if (message?.message) return 'text';
  return 'other';
}

function messageHasLink(message) {
  const text=String(message?.message||'');
  if(/(?:https?:\/\/|www\.|t\.me\/|telegram\.me\/|(?:[a-z0-9-]+\.)+(?:com|net|org|io|me|cc|tv|cn|co|top|xyz|site|info|pro|vip)(?:\/[^\s<>()]*)?)/i.test(text)) return true;
  const entities=Array.isArray(message?.entities)?message.entities:[];
  return entities.some(e=>/MessageEntity(?:Url|TextUrl|Email)/i.test(String(e?.className||e?.constructor?.name||'')));
}
function applyContentFiltersToText(text, filters) {
  let value=String(text||'');
  const blockKeywords=Array.isArray(filters?.block_keywords)?filters.block_keywords:[];
  if(blockKeywords.some(k=>String(k||'') && value.toLowerCase().includes(String(k).toLowerCase()))) return null;
  if(filters?.block_links===true && /(?:https?:\/\/|www\.|t\.me\/|telegram\.me\/|(?:[a-z0-9-]+\.)+(?:com|net|org|io|me|cc|tv|cn|co|top|xyz|site|info|pro|vip)(?:\/[^\s<>()]*)?)/i.test(value)) return null;
  if(filters?.remove_links===true) value=value.replace(/(?:https?:\/\/|www\.|t\.me\/|telegram\.me\/|(?:[a-z0-9-]+\.)+(?:com|net|org|io|me|cc|tv|cn|co|top|xyz|site|info|pro|vip)(?:\/[^\s<>()]*)?)/gi,'').replace(/[ \t]{2,}/g,' ').trim();
  const removeKeywords=Array.isArray(filters?.remove_keywords)?filters.remove_keywords:[];
  for(const k of removeKeywords){ const s=String(k||''); if(s) value=value.split(s).join(''); }
  const rules=Array.isArray(filters?.replace_rules)?filters.replace_rules:[];
  for(const rule of rules){ const from=String(rule?.from??''); const to=String(rule?.to??''); if(from) value=value.split(from).join(to); }
  return value;
}
function applyContentFiltersToMessage(message, filters) {
  if(!message)return null;
  const text=String(message.message||'');
  if(filters?.block_links===true && messageHasLink(message)) return null;
  if(!text){
    if(filters?.block_links===true && messageHasLink(message)) return null;
    return message;
  }
  const value=applyContentFiltersToText(text,filters);
  if(value===null)return null;
  if(value===text && !(filters?.remove_links===true && messageHasLink(message))) return message;
  const clone=Object.create(Object.getPrototypeOf(message));
  Object.assign(clone,message);
  clone.message=value;
  if('entities' in clone) clone.entities=[];
  return clone;
}
function shouldForwardMessage(message,filters){
  if(!message)return false;
  if(message.action && !message.message && !message.media) return false;
  if(!filters[getGramJsMessageType(message)])return false;
  return applyContentFiltersToMessage(message,filters)!==null;
}

function isAdmin(ctx) {
  return Number(ctx.from?.id) === adminId;
}

async function ensureBotUser(userId) {
  const uid = Number(userId);
  const p = await db();
  const [rows] = await p.query('SELECT status FROM bot_users WHERE user_id=? LIMIT 1', [uid]);
  if (!rows.length) {
    await p.query('INSERT INTO bot_users (user_id, status) VALUES (?, ?)', [uid, uid === adminId ? 'authorized' : 'pending']);
    return uid === adminId ? 'authorized' : 'pending';
  }
  if (uid === adminId && rows[0].status !== 'authorized') {
    await p.query('UPDATE bot_users SET status="authorized" WHERE user_id=?', [uid]);
    return 'authorized';
  }
  return String(rows[0].status);
}

async function forceLogoutUser(userId) {
  const uid = Number(userId);
  sessions.delete(uid);
  const client = userClients.get(uid);
  if (client) {
    try { await client.disconnect(); } catch {}
  }
  userClients.delete(uid);
  const p = await db();
  await p.query('DELETE FROM telegram_auth WHERE admin_id=?', [uid]);
}

async function getManagedUsers() {
  const p = await db();
  const [rows] = await p.query(
    `SELECT u.user_id, u.status, u.created_at, u.updated_at,
            EXISTS(SELECT 1 FROM telegram_auth a WHERE a.admin_id=u.user_id) AS logged_in,
            (SELECT COUNT(*) FROM forward_tasks t WHERE t.admin_id=u.user_id) AS task_count
     FROM bot_users u
     ORDER BY FIELD(u.status,'pending','authorized','disabled'), u.updated_at DESC
     LIMIT 100`
  );
  return rows;
}

bot.use(async (ctx, next) => {
  if (!ctx.from?.id) return next();
  const chatType = ctx.chat?.type;
  if (ctx.updateType !== 'callback_query' && chatType !== 'private') return next();

  const uid = Number(ctx.from.id);
  const status = await ensureBotUser(uid);
  if (status === 'authorized') return next();

  if (status === 'pending' && uid !== adminId && !botUserNotifications.has(uid)) {
    botUserNotifications.add(uid);
    try {
      await bot.telegram.sendMessage(
        adminId,
        `🔔 新用户请求使用转发机器人\n用户 ID：${uid}\n\n请进入“👑 用户管理”授权后才能使用。`
      );
    } catch {}
  }

  if (ctx.updateType === 'callback_query') {
    try { await ctx.answerCbQuery('⛔ 暂无使用权限'); } catch {}
  }
  return ctx.reply(
    status === 'disabled'
      ? '🚫 你的账号已被管理员禁用。'
      : `⛔ 暂未获得使用权限。\n\n你的 Telegram 用户 ID：${uid}\n请把这个 ID 发给机器人管理员申请授权。`
  );
});

function menu(userId) {
  const loggedIn=userClients.has(Number(userId));
  return Markup.inlineKeyboard([
    [Markup.button.callback(loggedIn?'✅ Telegram账号已登录':'🔐 Telegram账号登录','tg_login')],
    [Markup.button.callback('➕ 添加任务','add_task')],
    [Markup.button.callback('📚 全部历史克隆','history_all'),Markup.button.callback('📅 按日期时间克隆','history_dates'),Markup.button.callback('🕘 按消息 ID 范围克隆','set_history')],
    [Markup.button.callback('▶️ 开始同步','start_sync'),Markup.button.callback('⏸ 暂停同步','pause_sync')],
    [Markup.button.callback('🔄 实时转发','realtime'),Markup.button.callback('⚙️ 同步设置','sync_settings')],
    [Markup.button.callback('🎛 过滤设置','filters'),Markup.button.callback('📊 任务进度','progress')],
    [Markup.button.callback('📋 我的任务','tasks'),Markup.button.callback('🗑 删除任务','delete_task')],
    ...(loggedIn?[[Markup.button.callback('🔓 退出 Telegram账号','tg_logout')]]:[]),
    ...(Number(userId) === adminId?[[Markup.button.callback('👑 用户管理','admin_users')]]:[])
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
  if(!client)throw new Error('请先登录 Telegram');

  // 统一保存为 GramJS 官方的 Bot API 风格 ID：
  // 用户：123；普通群：-123；频道/超级群：-100123...
  // 不再手工使用 -1000000000000 + id，这会生成错误的频道 ID。
  const entity=await client.getEntity(cleaned);
  const peerId=await client.getPeerId(entity);
  const id=Number(peerId);
  if(!Number.isSafeInteger(id))throw new Error('无法获取有效的频道/群 ID');
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
  const media=Object.keys(labels).map(k => `${filters[k] ? '✅' : '❌'}${labels[k]}`).join('  ');
  const replaceCount=Array.isArray(filters.replace_rules)?filters.replace_rules.length:0;
  const blockCount=Array.isArray(filters.block_keywords)?filters.block_keywords.length:0;
  const removeCount=Array.isArray(filters.remove_keywords)?filters.remove_keywords.length:0;
  return media+`\n🔗屏蔽链接：${filters.block_links?'✅':'❌'}  🧹去除链接：${filters.remove_links?'✅':'❌'}\n🚫屏蔽关键词：${blockCount?'✅ '+blockCount+'条':'❌'}  🧹去除关键词：${removeCount?'✅ '+removeCount+'条':'❌'}\n🔄关键词替换：${replaceCount?'✅ '+replaceCount+'条':'❌'}`;
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

      for(let id=nextId;id<=batchEnd;id++){
        try{
          if(await isAlreadyForwarded(taskId,id)){
            // Older versions may have forwarded the post before comment mapping existed.
            if (source.className === 'Channel' && filters.clone_comments !== false) {
              const [mappedPost] = await p.query(
                'SELECT target_message_id FROM forwarded_messages WHERE task_id=? AND source_message_id=? AND target_message_id IS NOT NULL LIMIT 1',
                [taskId,id]
              );
              const mappedTargetId = Number(mappedPost[0]?.target_message_id || 0);
              if (mappedTargetId > 0) {
                try { await cloneDiscussionComments(client, task, source, id, mappedTargetId); }
                catch (repairErr) { console.error('补齐已有帖子评论映射失败',taskId,id,repairErr?.message||repairErr); }
              }
            }
            await p.query('UPDATE forward_tasks SET history_next_id=? WHERE id=? AND admin_id=?',[id+1,taskId,userId]);
            continue;
          }

          const got=await client.getMessages(source,{ids:[id]});
          const msg=Array.isArray(got)?got[0]:got;
          if(!msg){
            await p.query('UPDATE forward_tasks SET history_processed=history_processed+1,history_skipped=history_skipped+1,history_next_id=? WHERE id=? AND admin_id=?',[id+1,taskId,userId]);
            continue;
          }

          // 日期范围模式：仅转发所选 UTC+8 日期时间内的消息。
          const dateRange = task.history_start_date && task.history_end_date
            ? { start: new Date(task.history_start_date), end: new Date(task.history_end_date) }
            : null;
          if (dateRange && msg.date) {
            const messageDate = new Date(Number(msg.date) * 1000);
            if (messageDate < dateRange.start || messageDate > dateRange.end) {
              await p.query('UPDATE forward_tasks SET history_processed=history_processed+1,history_skipped=history_skipped+1,history_next_id=? WHERE id=? AND admin_id=?',[id+1,taskId,userId]);
              await markForwarded(taskId,id,null);
              continue;
            }
          }

          if(!shouldForwardMessage(msg,filters)){
            await p.query('UPDATE forward_tasks SET history_processed=history_processed+1,history_skipped=history_skipped+1,history_next_id=? WHERE id=? AND admin_id=?',[id+1,taskId,userId]);
            await markForwarded(taskId,id,null);
            continue;
          }

          let messages=[msg];
          if(msg.groupedId!=null){
            try{
              const around=await client.getMessages(source,{limit:30,around:Number(msg.id)});
              const album=around.filter(x=>x && x.groupedId!=null && String(x.groupedId)===String(msg.groupedId));
              if(album.length)messages=album.sort((a,b)=>Number(a.id)-Number(b.id));
            }catch(err){
              console.error('历史相册读取失败',taskId,id,err?.message||err);
            }
            let albumHasPriorForward = false;
            for(const item of messages){
              if(await isAlreadyForwarded(taskId,item.id)) { albumHasPriorForward = true; break; }
            }
            if (albumHasPriorForward) messages = [];
            if(!messages.length){
              await p.query('UPDATE forward_tasks SET history_next_id=? WHERE id=? AND admin_id=?',[id+1,taskId,userId]);
              continue;
            }
          }

          const topicEnabled=filters.clone_topics!==false;
          const out=await sendTelegramMessagesWithoutSource(
            client,
            source,
            target,
            messages,
            topicEnabled ? async item => ensureTargetForumTopic(client,task,source,target,getForumTopicId(item)) : null,
            async (item,topicId) => {
              if(filters.clone_comments===false)return 0;
              const sourceReplyId=getDirectReplyMessageId(item,getForumTopicId(item));
              if(!sourceReplyId)return 0;
              return await getForwardedTargetMessageId(taskId,sourceReplyId);
            },
            filters,
            userId
          );

          const arr=Array.isArray(out)?out:[];
          const forwardedBySource=new Map(arr.map(item=>[Number(item?.sourceId||0), Number(item?.sent?.id||0)]));
          for(const item of messages){
            const targetId=forwardedBySource.get(Number(item.id))||0;
            await markForwarded(taskId,item.id,targetId);
            if(targetId>0 && filters.clone_comments!==false && source.className==='Channel'){
              await cloneDiscussionComments(client, task, source, item.id, targetId);
            }
          }
          const sentMessageId=forwardedBySource.get(id)||0;
          await p.query(
            'UPDATE forward_tasks SET history_processed=history_processed+?,history_next_id=? WHERE id=? AND admin_id=?',
            [messages.length,id+1,taskId,userId]
          );
          console.log('历史同步完成',taskId,'源消息',messages.map(item=>item.id).join(','),'当前目标',sentMessageId);
        }catch(singleErr){
          if (singleErr?.code === 'DAILY_FORWARD_LIMIT') {
            console.warn('历史同步达到每日转发上限，暂停任务', taskId, singleErr.message);
            await p.query('UPDATE forward_tasks SET status="paused" WHERE id=? AND admin_id=?', [taskId, userId]);
            return;
          }
          const wait=getFloodWaitSeconds(singleErr);
          if(wait>0){
            console.error('历史同步遇到 Telegram 限流，暂停任务等待人工确认',taskId,id,wait,singleErr?.message||singleErr);
            await p.query('UPDATE forward_tasks SET status="paused" WHERE id=? AND admin_id=?', [taskId, userId]);
            return;
          }
          console.error('历史单条同步失败，继续下一条',taskId,id,singleErr?.message||singleErr);
          await p.query(
            'UPDATE forward_tasks SET history_processed=history_processed+1,history_failed=history_failed+1,history_next_id=? WHERE id=? AND admin_id=?',
            [id+1,taskId,userId]
          );
        }
      }

      nextId=batchEnd+1;
    }

    await p.query('UPDATE forward_tasks SET history_done=1,status="paused",history_next_id=history_end_id+1 WHERE id=? AND admin_id=?',[taskId,userId]);
    console.log('历史同步全部完成',taskId);
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


bot.action('admin_users', async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery('无权限');
  const rows = await getManagedUsers();
  await ctx.answerCbQuery();
  const pending = rows.filter(r => r.status === 'pending').length;
  const authorized = rows.filter(r => r.status === 'authorized').length;
  const disabled = rows.filter(r => r.status === 'disabled').length;
  const buttons = rows.map(r => [
    Markup.button.callback(
      `${r.status === 'pending' ? '⏳' : r.status === 'authorized' ? '✅' : '🚫'} ${r.user_id}｜任务 ${r.task_count}${Number(r.logged_in) ? '｜已登录' : ''}`,
      `admin_user_${r.user_id}`
    )
  ]);
  buttons.push([Markup.button.callback('➕ 授权用户','admin_authorize_prompt')]);
  buttons.push([Markup.button.callback('🏠 返回主页','menu_back')]);
  return ctx.reply(
    `👑 用户管理\n\n⏳ 待授权：${pending}\n✅ 已授权：${authorized}\n🚫 已禁用：${disabled}\n\n点击用户可管理权限。`,
    Markup.inlineKeyboard(buttons)
  );
});

bot.action('admin_authorize_prompt', async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery('无权限');
  sessions.set(adminId, { step: 'admin_authorize_user' });
  await ctx.answerCbQuery();
  return ctx.reply('➕ 授权用户\n\n请发送对方的 Telegram 用户 ID。');
});

bot.action(/^admin_user_(\d+)$/, async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery('无权限');
  const uid = Number(ctx.match[1]);
  if (uid === adminId) return ctx.answerCbQuery('管理员不能操作');
  const p = await db();
  const [rows] = await p.query(
    `SELECT u.user_id,u.status,
            EXISTS(SELECT 1 FROM telegram_auth a WHERE a.admin_id=u.user_id) AS logged_in,
            (SELECT COUNT(*) FROM forward_tasks t WHERE t.admin_id=u.user_id) AS task_count
     FROM bot_users u WHERE u.user_id=? LIMIT 1`,
    [uid]
  );
  if (!rows.length) return ctx.answerCbQuery('用户不存在');
  const u = rows[0];
  const buttons = [];
  if (u.status === 'authorized') {
    buttons.push([Markup.button.callback('🚫 禁用用户', `admin_disable_${uid}`)]);
    if (Number(u.logged_in)) buttons.push([Markup.button.callback('🔌 强制退出 Telegram', `admin_logout_${uid}`)]);
  } else {
    buttons.push([Markup.button.callback('✅ 授权用户', `admin_enable_${uid}`)]);
  }
  buttons.push([Markup.button.callback('🗑 移除用户', `admin_remove_${uid}`)]);
  buttons.push([Markup.button.callback('⬅️ 返回用户列表','admin_users')]);
  return ctx.reply(
    `👤 用户：${uid}\n状态：${u.status === 'authorized' ? '✅ 已授权' : u.status === 'disabled' ? '🚫 已禁用' : '⏳ 待授权'}\nTelegram 登录：${Number(u.logged_in) ? '✅ 是' : '❌ 否'}\n任务数量：${u.task_count}`,
    Markup.inlineKeyboard(buttons)
  );
});

bot.action(/^admin_enable_(\d+)$/, async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery('无权限');
  const uid = Number(ctx.match[1]);
  if (uid === adminId) return ctx.answerCbQuery('管理员不能操作');
  const p = await db();
  await p.query(
    `INSERT INTO bot_users (user_id,status) VALUES (?, 'authorized')
     ON DUPLICATE KEY UPDATE status='authorized'`,
    [uid]
  );
  await ctx.answerCbQuery('已授权');
  try { await bot.telegram.sendMessage(uid, '✅ 管理员已授权你使用此转发机器人，现在可以发送 /start。'); } catch {}
  return ctx.reply('✅ 用户 ' + uid + ' 已授权。', Markup.inlineKeyboard([
    [Markup.button.callback('⬅️ 返回用户列表','admin_users')],
    [Markup.button.callback('🏠 返回主页','menu_back')]
  ]));
});

bot.action(/^admin_disable_(\d+)$/, async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery('无权限');
  const uid = Number(ctx.match[1]);
  if (uid === adminId) return ctx.answerCbQuery('管理员不能操作');
  const p = await db();
  await p.query(
    `INSERT INTO bot_users (user_id,status) VALUES (?, 'disabled')
     ON DUPLICATE KEY UPDATE status='disabled'`,
    [uid]
  );
  await forceLogoutUser(uid);
  await ctx.answerCbQuery('已禁用并退出 Telegram');
  try { await bot.telegram.sendMessage(uid, '🚫 管理员已禁用你的使用权限，并退出了你的 Telegram 登录。'); } catch {}
  return ctx.reply('🚫 用户 ' + uid + ' 已禁用。', Markup.inlineKeyboard([
    [Markup.button.callback('⬅️ 返回用户列表','admin_users')],
    [Markup.button.callback('🏠 返回主页','menu_back')]
  ]));
});

bot.action(/^admin_logout_(\d+)$/, async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery('无权限');
  const uid = Number(ctx.match[1]);
  if (uid === adminId) return ctx.answerCbQuery('管理员不能操作');
  await forceLogoutUser(uid);
  await ctx.answerCbQuery('已强制退出');
  try { await bot.telegram.sendMessage(uid, '🔌 管理员已强制退出你的 Telegram 登录。任务不会删除。'); } catch {}
  return ctx.reply('🔌 用户 ' + uid + ' 已强制退出 Telegram。', Markup.inlineKeyboard([
    [Markup.button.callback('⬅️ 返回用户列表','admin_users')],
    [Markup.button.callback('🏠 返回主页','menu_back')]
  ]));
});

bot.action(/^admin_remove_(\d+)$/, async ctx => {
  if (!isAdmin(ctx)) return ctx.answerCbQuery('无权限');
  const uid = Number(ctx.match[1]);
  if (uid === adminId) return ctx.answerCbQuery('管理员不能操作');
  await forceLogoutUser(uid);
  const p = await db();
  await p.query('DELETE FROM bot_users WHERE user_id=?', [uid]);
  await ctx.answerCbQuery('已移除');
  try { await bot.telegram.sendMessage(uid, '🗑 你已被管理员从转发机器人的用户列表移除。'); } catch {}
  return ctx.reply('🗑 用户 ' + uid + ' 已移除。', Markup.inlineKeyboard([
    [Markup.button.callback('⬅️ 返回用户列表','admin_users')],
    [Markup.button.callback('🏠 返回主页','menu_back')]
  ]));
});

bot.start(async ctx=>ctx.reply('🤖 Telegram 转发机器人\n\n每个用户独立登录自己的 Telegram 账号。\n登录后可自行设置源频道、目标频道和同步任务。',menu(ctx.from.id)));
bot.command('menu',async ctx=>ctx.reply('🤖 主菜单',menu(ctx.from.id)));

bot.action('tg_login',async ctx=>{
  const uid=Number(ctx.from.id);
  await ctx.answerCbQuery();
  if(userClients.has(uid))return ctx.reply('✅ 你的 Telegram 账号已经登录。\n\n可以直接添加任务。',menu(uid));
  const current=sessions.get(uid);
  if(current?.step?.startsWith('tg_'))return ctx.reply('⏳ Telegram 登录流程正在进行中，请按当前提示继续；如正在扫码，请扫描最新二维码。');
  if(!TG_API_ID||!TG_API_HASH)return ctx.reply('❌ 服务器尚未配置 TG_API_ID / TG_API_HASH。\n\n普通用户不需要填写 API ID/API Hash，请管理员在 VPS 的 .env 中配置一次。');
  return ctx.reply('🔐 选择 Telegram 登录方式\n\n📱 手机号验证码登录：输入手机号和 Telegram 验证码。\n📷 扫码登录：用另一台已登录 Telegram 的设备扫描二维码并确认。',Markup.inlineKeyboard([
    [Markup.button.callback('📱 手机号验证码登录','tg_login_phone')],
    [Markup.button.callback('📷 扫码登录','tg_login_qr')],
    [Markup.button.callback('↩️ 返回登录方式','tg_login')]
  ]));
});

bot.action('tg_login_phone',async ctx=>{
  const uid=Number(ctx.from.id);
  await ctx.answerCbQuery();
  if(userClients.has(uid))return ctx.reply('✅ 你的 Telegram 账号已经登录。',menu(uid));
  const current=sessions.get(uid);
  if(current?.step?.startsWith('tg_'))return ctx.reply('⏳ 已有 Telegram 登录流程正在进行，请先完成当前流程。');
  if(!TG_API_ID||!TG_API_HASH)return ctx.reply('❌ 服务器尚未配置 TG_API_ID / TG_API_HASH。');
  sessions.set(uid,{step:'tg_phone'});
  return ctx.reply('📱 手机号验证码登录\n\n请输入 Telegram 手机号（含国家区号，例如 +8613812345678）。');
});

bot.action('tg_login_qr',async ctx=>{
  const uid=Number(ctx.from.id);
  await ctx.answerCbQuery();
  if(userClients.has(uid))return ctx.reply('✅ 你的 Telegram 账号已经登录。',menu(uid));
  const current=sessions.get(uid);
  if(current?.step?.startsWith('tg_'))return ctx.reply('⏳ 已有 Telegram 登录流程正在进行，请先完成当前流程。');
  if(!TG_API_ID||!TG_API_HASH)return ctx.reply('❌ 服务器尚未配置 TG_API_ID / TG_API_HASH。');
  sessions.set(uid,{step:'tg_qr_waiting'});
  runTelegramQrLogin(uid).catch(err=>{
    console.error('用户 Telegram 扫码登录失败',uid,err?.message||err);
    const active=sessions.get(uid);
    if(active?.step?.startsWith('tg_'))sessions.delete(uid);
    bot.telegram.sendMessage(uid,'❌ Telegram 扫码登录失败：'+(err?.message||err)+'\n\n请重新点击“🔐 Telegram账号登录”再试。',menu(uid)).catch(()=>{});
  });
  return ctx.reply('📷 正在生成 Telegram 登录二维码……\n\n请使用另一台已经登录 Telegram 的手机/电脑，在 Telegram 设置中的“设备”里选择“连接桌面设备”并扫描机器人发来的最新二维码。二维码会过期，请及时扫描。');
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
  return ctx.reply('➕ 添加转发任务\n\n第1步：发送【源频道/群】的 ID、@用户名或 t.me 链接。\n第2步：再发送【目标频道/群】。');
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
    menu(ctx.from.id)
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

bot.action('sync_settings', async ctx => {
  const rows = await getTasks(ctx.from.id);
  if (!rows.length) return ctx.answerCbQuery('没有任务');
  await ctx.answerCbQuery();
  return ctx.reply(
    '⚙️ 同步设置\n\n请选择要设置的任务：',
    Markup.inlineKeyboard([
      ...rows.map(t => [
        Markup.button.callback(`#${t.id} ${t.source_chat_id} → ${t.target_chat_id}`, `syncset_task_${t.id}`)
      ]),
      [Markup.button.callback('🏠 返回主页','menu_back')]
    ])
  );
});

bot.action(/^syncset_task_(\d+)$/, async ctx => {
  const taskId = Number(ctx.match[1]);
  const p = await db();
  const [rows] = await p.query('SELECT filters_json,realtime FROM forward_tasks WHERE id=? AND admin_id=?',[taskId,Number(ctx.from.id)]);
  if (!rows.length) return ctx.answerCbQuery('任务不存在');
  const settings = parseFilters(rows[0].filters_json);
  const topicClone = settings.clone_topics !== false;
  const commentClone = settings.clone_comments !== false;
  const realtime = Number(rows[0].realtime) === 1;
  await ctx.answerCbQuery();
  return ctx.reply(
    `⚙️ 任务 #${taskId} 同步设置\n\n🧵 话题 + 内容：${topicClone ? '✅ 克隆中' : '❌ 已关闭'}\n💬 评论区：${commentClone ? '✅ 克隆' : '❌ 不克隆'}\n🔄 实时同步：${realtime ? '✅ 开启' : '❌ 关闭'}`,
    Markup.inlineKeyboard([
      [Markup.button.callback(`🧵 话题 + 内容克隆 ${topicClone ? '✅' : '❌'}`,`syncset_${taskId}_topics`)],
      [Markup.button.callback(`💬 克隆评论区 ${commentClone ? '✅' : '❌'}`,`syncset_${taskId}_comments`)],
      [Markup.button.callback('🛠 补齐已有帖子评论',`syncset_${taskId}_repair_comments`)],
      [Markup.button.callback(`🔄 实时同步 ${realtime ? '✅' : '❌'}`,`syncset_${taskId}_realtime`)],
      [Markup.button.callback('⬅️ 返回主菜单','menu_back')]
    ])
  );
});

bot.action(/^syncset_(\d+)_repair_comments$/, async ctx => {
  const taskId = Number(ctx.match[1]);
  const uid = Number(ctx.from.id);

  // Acknowledge the callback before database/network work.
  try {
    await ctx.answerCbQuery('已收到');
  } catch (err) {
    console.warn('评论补齐按钮回调已过期，继续检查任务', err?.description || err?.message || err);
  }

  try {
    const p = await db();
    const [rows] = await p.query(
      'SELECT * FROM forward_tasks WHERE id=? AND admin_id=? LIMIT 1',
      [taskId, uid]
    );
    if (!rows.length) return ctx.reply('❌ 任务不存在或无权操作。');
    if (!userClients.has(uid)) return ctx.reply('❌ 请先登录 Telegram 账号，再补齐评论区。');
    if (parseFilters(rows[0].filters_json).clone_comments === false) {
      return ctx.reply('❌ 请先在同步设置中开启「克隆评论区」。');
    }
    if (discussionRepairJobs.has(taskId)) {
      return ctx.reply('⏳ 这个任务已经在补齐评论区，请勿重复点击。');
    }

    discussionRepairJobs.add(taskId);
    await ctx.reply(
      '🛠 已开始补齐任务 #' + taskId + ' 的历史评论区。\n\n' +
      '会逐条检查已转发的频道帖子，并尝试补齐评论映射和遗漏评论。任务可能需要一些时间，请保持机器人运行。'
    );
    repairDiscussionMapsForTask(rows[0], uid)
      .then(async result => {
        await bot.telegram.sendMessage(
          uid,
          '✅ 任务 #' + taskId + ' 评论区补齐检查完成。\n' +
          '已检查帖子：' + result.checked + '/' + result.total + '\n' +
          '处理异常：' + result.errors + '\n\n' +
          '没有可访问评论区或目标频道未关联讨论群的帖子会自动跳过。'
        );
      })
      .catch(async err => {
        console.error('历史评论区补齐任务失败', taskId, err?.message || err);
        try {
          await bot.telegram.sendMessage(uid, '❌ 任务 #' + taskId + ' 评论区补齐失败：' + (err?.message || err));
        } catch {}
      })
      .finally(() => discussionRepairJobs.delete(taskId));
  } catch (err) {
    console.error('处理评论区补齐按钮失败', taskId, err?.message || err);
    try {
      await ctx.reply('❌ 无法启动评论区补齐：' + (err?.message || err));
    } catch {}
  }
});

bot.action(/^syncset_(\d+)_(topics|comments)$/, async ctx => {
  const taskId = Number(ctx.match[1]);
  const kind = ctx.match[2];
  const p = await db();
  const [rows] = await p.query('SELECT filters_json FROM forward_tasks WHERE id=? AND admin_id=?',[taskId,Number(ctx.from.id)]);
  if (!rows.length) return ctx.answerCbQuery('任务不存在');
  const settings = parseFilters(rows[0].filters_json);
  const key = kind === 'topics' ? 'clone_topics' : 'clone_comments';
  settings[key] = settings[key] === false;
  await p.query('UPDATE forward_tasks SET filters_json=? WHERE id=? AND admin_id=?',[JSON.stringify(settings),taskId,Number(ctx.from.id)]);
  await ctx.answerCbQuery(settings[key] ? '已开启' : '已关闭');
  return ctx.reply(`⚙️ 任务 #${taskId}：${kind === 'topics' ? '话题 + 内容克隆' : '克隆评论区'} 已${settings[key] ? '开启' : '关闭'}。`,menu(ctx.from.id));
});

bot.action(/^syncset_(\d+)_realtime$/, async ctx => {
  const taskId = Number(ctx.match[1]);
  const p = await db();
  const [rows] = await p.query('SELECT realtime FROM forward_tasks WHERE id=? AND admin_id=?',[taskId,Number(ctx.from.id)]);
  if (!rows.length) return ctx.answerCbQuery('任务不存在');
  const enabled = Number(rows[0].realtime) !== 1;
  await p.query('UPDATE forward_tasks SET realtime=? WHERE id=? AND admin_id=?',[enabled ? 1 : 0,taskId,Number(ctx.from.id)]);
  await ctx.answerCbQuery(enabled ? '已开启' : '已关闭');
  return ctx.reply(`⚙️ 任务 #${taskId} 实时同步已${enabled ? '开启' : '关闭'}。`,menu(ctx.from.id));
});

bot.action('menu_back', async ctx => {
  await ctx.answerCbQuery();
  return ctx.reply('🤖 主菜单',menu(ctx.from.id));
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
      [Markup.button.callback(`🔗 屏蔽链接 ${filters.block_links ? '✅' : '❌'}`, `ft_${taskId}_block_links`), Markup.button.callback(`🧹 去除链接 ${filters.remove_links ? '✅' : '❌'}`, `ft_${taskId}_remove_links`)],
      [Markup.button.callback(`🚫 屏蔽关键词 ${Array.isArray(filters.block_keywords)&&filters.block_keywords.length ? '✅' : '❌'}`, `keyword_rules_${taskId}_block`)],
      [Markup.button.callback(`🧹 去除关键词 ${Array.isArray(filters.remove_keywords)&&filters.remove_keywords.length ? '✅' : '❌'}`, `keyword_rules_${taskId}_remove`)],
      [Markup.button.callback('🔄 关键词替换', `replace_rules_${taskId}`)],
      [Markup.button.callback('⬅️ 返回', 'filters')]
    ])
  );
});

bot.action(/^ft_(\d+)_(text|photo|video|document|audio|voice|animation|sticker|video_note|other|block_links|remove_links)$/, async ctx => {
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

bot.action(/^keyword_rules_(\d+)_(block|remove)$/, async ctx => {
  const taskId=Number(ctx.match[1]), mode=ctx.match[2], uid=Number(ctx.from.id);
  const p=await db(); const [rows]=await p.query('SELECT filters_json FROM forward_tasks WHERE id=? AND admin_id=?',[taskId,uid]);
  if(!rows.length)return ctx.answerCbQuery('任务不存在');
  const f=parseFilters(rows[0].filters_json);
  const key=mode==='block'?'block_keywords':'remove_keywords';
  const list=Array.isArray(f[key])?f[key]:[];
  sessions.set(uid,{step:'keyword_rule',taskId,mode});
  const title=mode==='block'?'🚫 屏蔽关键词':'🧹 去除关键词';
  const shown=list.length?list.map((x,i)=>`${i+1}. ${x}`).join('\n'):'暂无关键词';
  await ctx.answerCbQuery();
  return ctx.reply(`${title}\n\n${shown}\n\n请发送一个关键词，每行一个也可以连续添加。发送“完成”结束。`,
    Markup.inlineKeyboard([[Markup.button.callback('🗑 清空全部',`keyword_clear_${taskId}_${mode}`)],[Markup.button.callback('⬅️ 返回','filters')]]));
});
bot.action(/^keyword_clear_(\d+)_(block|remove)$/, async ctx => {
  const taskId=Number(ctx.match[1]), mode=ctx.match[2], uid=Number(ctx.from.id);
  const p=await db(); const [rows]=await p.query('SELECT filters_json FROM forward_tasks WHERE id=? AND admin_id=?',[taskId,uid]);
  if(!rows.length)return ctx.answerCbQuery('任务不存在');
  const f=parseFilters(rows[0].filters_json); f[mode==='block'?'block_keywords':'remove_keywords']=[];
  await p.query('UPDATE forward_tasks SET filters_json=? WHERE id=? AND admin_id=?',[JSON.stringify(f),taskId,uid]);
  sessions.delete(uid); await ctx.answerCbQuery('已清空'); return ctx.reply('✅ 关键词已清空。',menu(uid));
});

bot.action(/^replace_rules_(\d+)$/, async ctx => {
  const taskId=Number(ctx.match[1]); const uid=Number(ctx.from.id);
  const p=await db(); const [rows]=await p.query('SELECT filters_json FROM forward_tasks WHERE id=? AND admin_id=?',[taskId,uid]);
  if(!rows.length)return ctx.answerCbQuery('任务不存在');
  sessions.set(uid,{step:'replace_rule',taskId});
  const f=parseFilters(rows[0].filters_json), rules=Array.isArray(f.replace_rules)?f.replace_rules:[];
  const list=rules.length?rules.map((x,i)=>`${i+1}. ${x.from} → ${x.to}`).join('\n'):'暂无替换规则';
  await ctx.answerCbQuery();
  return ctx.reply(`🔄 关键词/链接替换\n\n${list}\n\n发送：原关键词 => 新关键词\n例如：旧域名.com => 新域名.com\n可连续添加，发送“完成”结束。`,
    Markup.inlineKeyboard([[Markup.button.callback('🗑 清空全部替换','replace_clear_'+taskId)],[Markup.button.callback('⬅️ 返回','filters')]]));
});
bot.action(/^replace_clear_(\d+)$/, async ctx => {
  const taskId=Number(ctx.match[1]),uid=Number(ctx.from.id); const p=await db();
  const [rows]=await p.query('SELECT filters_json FROM forward_tasks WHERE id=? AND admin_id=?',[taskId,uid]);
  if(!rows.length)return ctx.answerCbQuery('任务不存在');
  const f=parseFilters(rows[0].filters_json); f.replace_rules=[];
  await p.query('UPDATE forward_tasks SET filters_json=? WHERE id=? AND admin_id=?',[JSON.stringify(f),taskId,uid]);
  sessions.delete(uid); await ctx.answerCbQuery('已清空'); return ctx.reply('✅ 替换规则已清空。',menu(uid));
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


function extractHistoryMessageId(message) {
  const origin = message?.forward_origin;
  if (origin?.type === 'channel' && Number(origin.message_id) > 0) return Number(origin.message_id);
  if (Number(message?.forward_from_message_id) > 0) return Number(message.forward_from_message_id);
  return null;
}

function extractHistoryIdsFromText(text) {
  const value = String(text || '').trim();
  const ids = [];
  const linkRe = /https?:\/\/t\.me\/(?:c\/\d+|[A-Za-z0-9_]+)\/(\d+)/gi;
  for (const match of value.matchAll(linkRe)) {
    const id = Number(match[1]);
    if (Number.isInteger(id) && id > 0) ids.push(id);
  }
  if (ids.length) return [...new Set(ids)];
  const nums = value.split(/\s+/).filter(Boolean).map(Number);
  if (nums.length && nums.every(Number.isInteger) && nums.every(n => n > 0)) return [...new Set(nums)];
  return [];
}

bot.action('history_all', async ctx => {
  const uid = Number(ctx.from.id);
  const client = userClients.get(uid);
  if (!client) {
    await ctx.answerCbQuery('请先登录 Telegram');
    return ctx.reply('❌ 请先登录 Telegram 账号。', menu(uid));
  }
  const rows = await getTasks(uid);
  if (!rows.length) return ctx.answerCbQuery('没有任务');
  await ctx.answerCbQuery();
  return ctx.reply(
    '📚 全部历史克隆\n请选择要克隆全部历史消息的任务：',
    Markup.inlineKeyboard(rows.map(t => [
      Markup.button.callback('#' + t.id + ' ' + t.source_chat_id + ' → ' + t.target_chat_id, 'history_all_task_' + t.id)
    ]))
  );
});

bot.action(/^history_all_task_(\d+)$/, async ctx => {
  const uid = Number(ctx.from.id);
  const taskId = Number(ctx.match[1]);
  const client = userClients.get(uid);
  if (!client) {
    await ctx.answerCbQuery('请先登录 Telegram');
    return ctx.reply('❌ 请先登录 Telegram 账号。', menu(uid));
  }
  const p = await db();
  const [rows] = await p.query('SELECT * FROM forward_tasks WHERE id=? AND admin_id=?', [taskId, uid]);
  if (!rows.length) return ctx.answerCbQuery('任务不存在');
  const task = rows[0];
  try {
    const source = await client.getEntity(Number(task.source_chat_id));
    const latest = await client.getMessages(source, { limit: 1 });
    const latestId = Number((Array.isArray(latest) ? latest[0] : latest)?.id || 0);
    if (!latestId) {
      await ctx.answerCbQuery('源频道没有可读取的消息');
      return ctx.reply('❌ 无法读取源频道最新消息，请确认账号有访问权限。', menu(uid));
    }
    await p.query(
      'UPDATE forward_tasks SET history_next_id=1,history_end_id=?,history_total=?,history_start_date=NULL,history_end_date=NULL,history_processed=0,history_skipped=0,history_failed=0,history_done=0,status="paused" WHERE id=? AND admin_id=?',
      [latestId, latestId, taskId, uid]
    );
    await ctx.answerCbQuery('已设置全部历史');
    return ctx.reply(
      '✅ 已设置全部历史克隆\n任务：#' + taskId + '\n消息范围：1 → ' + latestId + '\n\n已有转发记录会跳过，未转发的消息会继续克隆。点击“▶️ 开始同步”启动。',
      menu(uid)
    );
  } catch (err) {
    console.error('设置全部历史范围失败', taskId, err?.message || err);
    await ctx.answerCbQuery('读取源频道失败');
    return ctx.reply('❌ 读取源频道失败：' + String(err?.message || err).slice(0, 300), menu(uid));
  }
});

bot.action('history_dates', async ctx => {
  const rows = await getTasks(ctx.from.id);
  if (!rows.length) return ctx.answerCbQuery('没有任务');
  await ctx.answerCbQuery();
  return ctx.reply(
    '📅 按日期时间克隆历史消息\n\n请选择任务：',
    Markup.inlineKeyboard(rows.map(t => [
      Markup.button.callback('#' + t.id + ' ' + t.source_chat_id + ' → ' + t.target_chat_id, 'history_dates_task_' + t.id)
    ]))
  );
});

bot.action(/^history_dates_task_(\d+)$/, async ctx => {
  const uid = Number(ctx.from.id);
  const taskId = Number(ctx.match[1]);
  if (!userClients.has(uid)) {
    await ctx.answerCbQuery('请先登录 Telegram');
    return ctx.reply('❌ 请先登录 Telegram 账号。', menu(uid));
  }
  const p = await db();
  const [rows] = await p.query('SELECT id FROM forward_tasks WHERE id=? AND admin_id=?', [taskId, uid]);
  if (!rows.length) return ctx.answerCbQuery('任务不存在');
  sessions.set(uid, { step: 'history_date_start', taskId });
  await ctx.answerCbQuery();
  return ctx.reply(
    '📅 设置日期时间范围\n\n请发送开始时间和结束时间，每行一个，使用 24 小时制：\n\n2026-10-01 00:00\n2026-10-10 23:59\n\n按服务器时间解释为 UTC+8。请确认结束时间晚于开始时间。'
  );
});

bot.action('set_history', async ctx => {
  const rows = await getTasks(ctx.from.id);
  if (!rows.length) return ctx.answerCbQuery('没有任务');
  await ctx.answerCbQuery();
  return ctx.reply(
    '🕘 设置历史范围\n\n先选择要设置的任务：',
    Markup.inlineKeyboard(rows.map(t => [
      Markup.button.callback(`#${t.id} ${t.source_chat_id} → ${t.target_chat_id}`, `history_task_${t.id}`)
    ]))
  );
});

bot.action(/^history_task_(\d+)$/, async ctx => {
  const taskId = Number(ctx.match[1]);
  const uid = Number(ctx.from.id);
  const p = await db();
  const [rows] = await p.query('SELECT * FROM forward_tasks WHERE id=? AND admin_id=?', [taskId, uid]);
  if (!rows.length) return ctx.answerCbQuery('任务不存在');
  sessions.set(uid, { step: 'history_start', taskId });
  await ctx.answerCbQuery();
  return ctx.reply(
    '🕘 设置历史范围\n\n请发送【起始消息】。\n\n支持：\n• 直接转发源频道的一条消息给我\n• 粘贴消息链接\n• 直接发送消息 ID\n\n收到起点后，我再让你发送结束消息。'
  );
});


async function processHistoryDateInput(ctx) {
  const uid = Number(ctx.from.id);
  const session = sessions.get(uid);
  if (!session || session.step !== 'history_date_start') return false;
  const lines = String(ctx.message?.text || '').trim().split(/\r?\n/).map(x => x.trim()).filter(Boolean);
  const parseDate = value => {
    const m = value.match(/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2}))?$/);
    if (!m) return null;
    const [_, y, mo, d, h, mi, sec = '0'] = m;
    const date = new Date(Date.UTC(Number(y), Number(mo) - 1, Number(d), Number(h) - 8, Number(mi), Number(sec)));
    if (!Number.isFinite(date.getTime())) return null;
    return date;
  };
  if (lines.length < 2) {
    await ctx.reply('❌ 请发送两行时间：\n2026-10-01 00:00\n2026-10-10 23:59');
    return true;
  }
  const startDate = parseDate(lines[0]);
  const endDate = parseDate(lines[1]);
  if (!startDate || !endDate || endDate <= startDate) {
    await ctx.reply('❌ 时间格式不正确或结束时间早于开始时间。请使用 YYYY-MM-DD HH:mm，每行一个时间。');
    return true;
  }
  const taskId = Number(session.taskId);
  const p = await db();
  const [tasks] = await p.query('SELECT * FROM forward_tasks WHERE id=? AND admin_id=?', [taskId, uid]);
  if (!tasks.length) {
    sessions.delete(uid);
    await ctx.reply('❌ 找不到任务。', menu(uid));
    return true;
  }
  try {
    const client = userClients.get(uid);
    const source = await client.getEntity(Number(tasks[0].source_chat_id));
    // 找到起始日期之后的第一条消息，以及结束日期之前的最后一条消息。
    // 先用 Telegram 的日期游标定位，再用消息时间二次过滤，避免跨界消息误转。
    let firstId = 0;
    for await (const msg of client.iterMessages(source, { reverse: true, offsetDate: startDate, limit: 1 })) {
      if (msg && Number(msg.id) > 0) firstId = Number(msg.id);
    }
    const endResults = await client.getMessages(source, { limit: 1, offsetDate: new Date(endDate.getTime() + 1000) });
    let lastId = Number((Array.isArray(endResults) ? endResults[0] : endResults)?.id || 0);
    if (!firstId || !lastId || lastId < firstId) {
      await ctx.reply('❌ 这个日期范围内没有找到消息，或账号无法读取该时间段。请检查日期并重试。', menu(uid));
      return true;
    }
    await p.query(
      'UPDATE forward_tasks SET history_next_id=?,history_end_id=?,history_total=?,history_start_date=?,history_end_date=?,history_processed=0,history_skipped=0,history_failed=0,history_done=0,status="paused" WHERE id=? AND admin_id=?',
      [firstId, lastId, lastId - firstId + 1, startDate, endDate, taskId, uid]
    );
    sessions.delete(uid);
    await ctx.reply(
      '✅ 已设置日期时间范围\n任务：#' + taskId +
      '\n开始：' + lines[0] + '\n结束：' + lines[1] +
      '\n消息 ID 范围：' + firstId + ' → ' + lastId +
      '\n\n注意：同步时还会按消息实际时间检查范围，避免转发时间段之外的消息。点击“▶️ 开始同步”启动。',
      menu(uid)
    );
  } catch (err) {
    console.error('设置日期历史范围失败', taskId, err?.message || err);
    await ctx.reply('❌ 读取源频道时间范围失败：' + String(err?.message || err).slice(0, 250), menu(uid));
  }
  return true;
}

async function processHistoryInput(ctx) {
  const uid=Number(ctx.from.id);
  const session=sessions.get(uid);
  if (session?.step === 'history_date_start') return processHistoryDateInput(ctx);
  if (!session || (session.step!=='history_start' && session.step!=='history_end')) return false;

  const forwardedId=extractHistoryMessageId(ctx.message);
  const ids=forwardedId ? [forwardedId] : extractHistoryIdsFromText(ctx.message?.text);
  if (!ids.length) {
    await ctx.reply('❌ 没识别到消息。请直接转发源频道消息、粘贴 t.me 消息链接，或发送消息 ID。');
    return true;
  }

  const taskId=Number(session.taskId);
  const p=await db();
  const [rows]=await p.query('SELECT * FROM forward_tasks WHERE id=? AND admin_id=?',[taskId,uid]);
  if(!rows.length){
    sessions.delete(uid);
    await ctx.reply('❌ 找不到这个任务。',menu(uid));
    return true;
  }

  if(session.step==='history_start'){
    if(ids.length>=2){
      const startId=Number(ids[0]), endId=Number(ids[1]);
      if(endId<startId){ await ctx.reply('❌ 结束消息不能小于起始消息，请重新发送。'); return true; }
      const total=endId-startId+1;
      await p.query('UPDATE forward_tasks SET history_next_id=?,history_end_id=?,history_total=?,history_start_date=NULL,history_end_date=NULL,history_processed=0,history_skipped=0,history_failed=0,history_done=0,status="paused" WHERE id=? AND admin_id=?',
        [startId,endId,total,taskId,uid]);
      sessions.delete(uid);
      await ctx.reply('✅ 已设置任务 #'+taskId+'\n历史范围：'+startId+' → '+endId+'\n总数：'+total+'\n\n现在点击“▶️ 开始同步”。',menu(uid));
      return true;
    }
    session.historyStartId=Number(ids[0]);
    session.step='history_end';
    await ctx.reply('✅ 已收到起始消息：'+session.historyStartId+'\n\n现在请发送【结束消息】：\n• 直接转发一条源频道消息\n• 粘贴消息链接\n• 发送消息 ID');
    return true;
  }

  const startId=Number(session.historyStartId), endId=Number(ids[0]);
  if(!Number.isInteger(startId)||!Number.isInteger(endId)||startId<1||endId<startId){
    await ctx.reply('❌ 结束消息必须不小于起始消息（当前起点：'+startId+'）。请重新发送结束消息。');
    return true;
  }
  const total=endId-startId+1;
  await p.query('UPDATE forward_tasks SET history_next_id=?,history_end_id=?,history_total=?,history_start_date=NULL,history_end_date=NULL,history_processed=0,history_skipped=0,history_failed=0,history_done=0,status="paused" WHERE id=? AND admin_id=?',
    [startId,endId,total,taskId,uid]);
  sessions.delete(uid);
  await ctx.reply('✅ 已设置任务 #'+taskId+'\n历史范围：'+startId+' → '+endId+'\n总数：'+total+'\n\n现在点击“▶️ 开始同步”。',menu(uid));
  return true;
}

bot.on('text', async (ctx, next) => {
  const uid=Number(ctx.from.id);
  const session = sessions.get(uid);
    if (session?.step === 'keyword_rule') {
    const value=String(ctx.message?.text||'').trim();
    if(value==='完成'){sessions.delete(uid);return ctx.reply('✅ 关键词设置已保存。',menu(uid));}
    const values=value.split(/\r?\n|,/).map(x=>x.trim()).filter(Boolean);
    if(!values.length)return ctx.reply('❌ 请输入关键词。');
    const p=await db(); const [rows]=await p.query('SELECT filters_json FROM forward_tasks WHERE id=? AND admin_id=?',[session.taskId,uid]);
    if(!rows.length){sessions.delete(uid);return ctx.reply('任务不存在',menu(uid));}
    const f=parseFilters(rows[0].filters_json); const key=session.mode==='block'?'block_keywords':'remove_keywords';
    f[key]=Array.isArray(f[key])?f[key]:[];
    for(const v of values)if(!f[key].includes(v))f[key].push(v);
    await p.query('UPDATE forward_tasks SET filters_json=? WHERE id=? AND admin_id=?',[JSON.stringify(f),session.taskId,uid]);
    return ctx.reply(`✅ 已添加关键词：${values.join('、')}\n继续发送，或发送“完成”。`);
  }

  if (session?.step === 'replace_rule') {
      const value=String(ctx.message?.text||'').trim();
      if(value==='完成'){sessions.delete(uid);return ctx.reply('✅ 替换规则已保存。',menu(uid));}
      const m=value.match(/^(.+?)\s*(?:=>|->|＝>|→)\s*(.*)$/);
      if(!m)return ctx.reply('格式不正确，请使用：原关键词 => 新关键词');
      const p=await db(); const [rows]=await p.query('SELECT filters_json FROM forward_tasks WHERE id=? AND admin_id=?',[session.taskId,uid]);
      if(!rows.length){sessions.delete(uid);return ctx.reply('任务不存在',menu(uid));}
      const f=parseFilters(rows[0].filters_json); f.replace_rules=Array.isArray(f.replace_rules)?f.replace_rules:[];
      f.replace_rules.push({from:m[1],to:m[2]});
      await p.query('UPDATE forward_tasks SET filters_json=? WHERE id=? AND admin_id=?',[JSON.stringify(f),session.taskId,uid]);
      return ctx.reply(`✅ 已添加：${m[1]} → ${m[2]}\n继续发送下一条，或发送“完成”。`);
    }


  if (session?.step === 'admin_authorize_user') {
    if (uid !== adminId) {
      sessions.delete(uid);
      return ctx.reply('⛔ 无权限。');
    }
    const targetId = Number(String(ctx.message.text || '').trim());
    if (!Number.isSafeInteger(targetId) || targetId <= 0) {
      return ctx.reply('❌ 用户 ID 格式不正确，请重新发送数字 ID。');
    }
    const p = await db();
    await p.query(
      `INSERT INTO bot_users (user_id,status) VALUES (?, 'authorized')
       ON DUPLICATE KEY UPDATE status='authorized'`,
      [targetId]
    );
    sessions.delete(uid);
    try { await bot.telegram.sendMessage(targetId, '✅ 管理员已授权你使用此转发机器人，现在可以发送 /start。'); } catch {}
    return ctx.reply(`✅ 用户 ${targetId} 已授权。\n\n你可以继续在“👑 用户管理”里管理其他用户。`, menu(uid));
  }

  if (!session) return next();

  if(session.step==='tg_phone'){
    const rawPhone=String(ctx.message.text||'').trim();
    const phone=rawPhone.replace(/[^\d+]/g,'');
    if(!phone)return ctx.reply('❌ 请输入手机号。');
    session.phone=phone;
    // 先同步切换状态，再启动异步请求，避免连续消息触发两个独立登录客户端。
    session.step='tg_requesting_code';
    runTelegramBotLogin(uid).catch(err=>{
      console.error('用户 Telegram 登录失败',uid,err?.message||err);
      sessions.delete(uid);
      bot.telegram.sendMessage(uid,`❌ Telegram 登录失败：${err?.message||err}\n\n请重新点击“🔐 Telegram账号登录”。`,menu(uid)).catch(()=>{});
    });
    return ctx.reply('⏳ 正在请求 Telegram 验证码，请稍候……');
  }
  if(session.step==='tg_code'){
    const resolve=session.codeResolve;session.codeResolve=null;if(resolve)resolve(String(ctx.message.text).trim());return;
  }
  if(session.step==='tg_password'){
    const resolve=session.passwordResolve;session.passwordResolve=null;if(resolve)resolve(String(ctx.message.text));return;
  }
  if(session.step==='tg_qr_waiting'){
    return ctx.reply('📷 扫码登录正在等待确认，请扫描机器人发送的最新二维码；如果只使用这一台手机，请返回选择“手机号验证码登录”。');
  }
  if(session.step==='tg_requesting_code'){
    return ctx.reply('⏳ 正在请求 Telegram 验证码，请勿重复发送手机号；收到验证码后再发送验证码。');
  }


  if (session.step === 'history_date_start') {
    return processHistoryDateInput(ctx);
  }

  if (session.step === 'history_start' || session.step === 'history_end') {
    return processHistoryInput(ctx);
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
        [uid, session.source, target, JSON.stringify({...DEFAULT_FILTERS, clone_topics:true, clone_comments:true})]
      );

      const source = session.source;
      sessions.delete(ctx.from.id);
      return ctx.reply(
        `✅ 转发任务已添加\n\n源：${source}\n目标：${target}\n\n如需历史消息，请先设置历史范围；实时转发默认开启。`,
        menu(ctx.from.id)
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

  const phone=session.phone;
  const client=new TelegramClient(new StringSession(''),Number(TG_API_ID),TG_API_HASH,{connectionRetries:10});
  try{
    await bot.telegram.sendMessage(uid,'⏳ 正在连接 Telegram，请稍候……');
    await client.connect();
    console.log('Telegram 登录：连接成功',uid);

    if(await client.checkAuthorization()){
      await saveTelegramAuth(uid,TG_API_ID,TG_API_HASH,client.session.save());
      await attachTelegramEvents(client,uid);
      userClients.set(uid,client);
      sessions.delete(uid);
      return bot.telegram.sendMessage(uid,'✅ Telegram账号登录成功！\n\n以后你的任务都会使用这个 Telegram 账号执行。\nVPS 重启后会自动恢复登录状态。',menu(uid));
    }

    console.log('Telegram 登录：开始请求验证码',uid,phone);
    const sent=await Promise.race([
      client.sendCode({apiId:Number(TG_API_ID),apiHash:TG_API_HASH},phone),
      new Promise((_,reject)=>setTimeout(()=>reject(new Error('向 Telegram 请求验证码超过 60 秒仍未返回。')),60000))
    ]);

    if(!sent?.phoneCodeHash)throw new Error('Telegram 没有返回有效的验证码请求结果。');
    session.phoneCodeHash=sent.phoneCodeHash;
    console.log('Telegram 登录：验证码请求成功，等待用户输入',uid);

    const code=await waitTelegramInput(
      uid,
      'tg_code',
      'codeResolve',
      '📲 Telegram 验证码请求已成功发送。\n\n请查看你其他已登录设备里的“Telegram”官方服务消息；如果 Telegram 显示验证码，也可以直接把验证码发给我。'
    );

    let signedIn=false;
    try{
      await client.invoke(new Api.auth.SignIn({
        phoneNumber:phone,
        phoneCodeHash:session.phoneCodeHash,
        phoneCode:String(code).trim()
      }));
      signedIn=true;
    }catch(err){
      const name=String(err?.errorMessage||err?.message||'');
      if(!/SESSION_PASSWORD_NEEDED/i.test(name))throw err;
    }

    if(!signedIn){
      const password=await waitTelegramInput(
        uid,
        'tg_password',
        'passwordResolve',
        '🔐 你的 Telegram 账号开启了两步验证，请输入两步验证密码。'
      );
      await client.invoke(new Api.auth.CheckPassword({
        password:await client.computePasswordHash(String(password))
      }));
    }

    await saveTelegramAuth(uid,TG_API_ID,TG_API_HASH,client.session.save());
    const old=userClients.get(uid);
    if(old){try{await old.disconnect();}catch{}}
    await repairLegacyTaskChatIds(uid);
    await attachTelegramEvents(client,uid);
    userClients.set(uid,client);
    sessions.delete(uid);
    console.log('Telegram 登录成功',uid);
    return bot.telegram.sendMessage(uid,'✅ Telegram账号登录成功！\n\n以后你的任务都会使用这个 Telegram 账号执行。\nVPS 重启后会自动恢复登录状态。',menu(uid));
  }catch(err){
    try{await client.disconnect();}catch{}
    throw err;
  }
}

async function runTelegramQrLogin(userId){
  const uid=Number(userId),session=sessions.get(Number(userId));
  if(!session||session.step!=='tg_qr_waiting')throw new Error('扫码登录会话不存在或已结束');
  if(!TG_API_ID||!TG_API_HASH)throw new Error('服务器没有配置 TG_API_ID / TG_API_HASH');
  const client=new TelegramClient(new StringSession(''),Number(TG_API_ID),TG_API_HASH,{connectionRetries:10});
  try{
    await client.connect();
    if(await client.checkAuthorization()){
      await saveTelegramAuth(uid,TG_API_ID,TG_API_HASH,client.session.save());
      const old=userClients.get(uid);
      if(old){try{await old.disconnect();}catch{}}
      await repairLegacyTaskChatIds(uid);
      await attachTelegramEvents(client,uid);
      userClients.set(uid,client);
      sessions.delete(uid);
      return bot.telegram.sendMessage(uid,'✅ Telegram账号登录成功！\n\n以后你的任务都会使用这个 Telegram 账号执行，VPS 重启后会自动恢复登录状态。',menu(uid));
    }
    await bot.telegram.sendMessage(uid,'📷 请扫描下方二维码登录。\n\n只扫描你自己发起的登录二维码；请勿把二维码转发给他人。若出现多张二维码，请扫描最新一张。');
    await client.signInUserWithQrCode({apiId:Number(TG_API_ID),apiHash:TG_API_HASH},{
      qrCode:async({token,expires})=>{
        const current=sessions.get(uid);
        if(!current||!current.step?.startsWith('tg_'))throw new Error('扫码登录已取消');
        const deepLink='tg://login?token='+Buffer.from(token).toString('base64url');
        const imageBuffer=await QRCode.toBuffer(deepLink,{type:'png',width:360,margin:2,errorCorrectionLevel:'M'});
        await bot.telegram.sendPhoto(uid,{source:imageBuffer,filename:'telegram-login-qr.png'},{caption:'📷 Telegram 扫码登录\n\n请在二维码有效期内，用另一台已登录 Telegram 的设备扫描并确认。\n过期后请扫描后续发来的最新二维码。'});
        console.log('Telegram 扫码二维码已生成',uid,'expires',expires);
      },
      password:async(hint)=>{
        let prompt='🔐 此 Telegram 账号启用了两步验证。请输入两步验证密码。';
        if(hint)prompt+='\n密码提示：'+hint;
        const password=await waitTelegramInput(uid,'tg_password','passwordResolve',prompt);
        return String(password);
      },
      onError:async(err)=>{
        console.error('Telegram 扫码授权流程错误',uid,err?.message||err);
        return true;
      }
    });
    await saveTelegramAuth(uid,TG_API_ID,TG_API_HASH,client.session.save());
    const old=userClients.get(uid);
    if(old){try{await old.disconnect();}catch{}}
    await repairLegacyTaskChatIds(uid);
    await attachTelegramEvents(client,uid);
    userClients.set(uid,client);
    sessions.delete(uid);
    console.log('Telegram 扫码登录成功',uid);
    return bot.telegram.sendMessage(uid,'✅ Telegram账号扫码登录成功！\n\n以后你的任务都会使用这个 Telegram 账号执行，VPS 重启后会自动恢复登录状态。',menu(uid));
  }catch(err){
    try{await client.disconnect();}catch{}
    throw err;
  }
}

async function restoreAllTelegramClients(){
  const p=await db();
  const [rows]=await p.query(`SELECT a.admin_id,a.api_id,a.api_hash,a.tg_session
    FROM telegram_auth a
    LEFT JOIN bot_users u ON u.user_id=a.admin_id
    WHERE a.admin_id=? OR u.status='authorized'`, [adminId]);
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

    // 已登录 Telegram 账号的任务统一走 MTProto 实时克隆，保留话题与回复结构。
    if (userClients.has(Number(task.admin_id))) continue;

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

bot.on('message', async (ctx, next) => {
  try {
    if (ctx.chat?.type === 'private' && await processHistoryInput(ctx)) return;
  } catch (err) {
    console.error('私聊历史范围处理失败', err?.message || err);
  }
  return next();
});

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
  for (let attempt = 0; attempt < 8; attempt++) {
    try {
      return await bot.telegram.copyMessage(target, source, messageId);
    } catch (err) {
      const retryAfter = Number(
        err?.response?.parameters?.retry_after ||
        err?.parameters?.retry_after ||
        getFloodWaitSeconds(err) ||
        0
      );
      if (retryAfter > 0 && retryAfter <= 180) {
        await sleep(retryAfter * 1000 + 500);
        continue;
      }
      throw err;
    }
  }
  throw new Error('copyMessage 连续限流，已停止重试');
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