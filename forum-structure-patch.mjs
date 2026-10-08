import fs from 'fs';
import { execSync } from 'child_process';

const path = 'src/index.js';
let s = fs.readFileSync(path, 'utf8');
if (s.includes('// FORUM_STRUCTURE_CLONE_V1')) {
  console.log('话题群结构补丁已经应用，无需重复修改。');
  process.exit(0);
}

const dbAnchor = `    await pool.query(\`
      CREATE TABLE IF NOT EXISTS telegram_auth (`;
const dbInsert = `    await pool.query(\`
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
    \`);

    await pool.query(\`
      CREATE TABLE IF NOT EXISTS telegram_auth (`;
if (!s.includes(dbAnchor)) throw new Error('找不到数据库初始化位置');
s = s.replace(dbAnchor, dbInsert, 1);

const anchor = 'async function sendTelegramMessagesWithoutSource(client, target, messages) {';
const helper = `// FORUM_STRUCTURE_CLONE_V1
function getForumTopicId(message) {
  const reply = message?.replyTo;
  const top = Number(reply?.replyToTopId || 0);
  if (top > 0) return top;
  if (reply?.forumTopic && Number(reply?.replyToMsgId) > 0) return Number(reply.replyToMsgId);
  if (message?.action?.className === 'MessageActionTopicCreate' && Number(message?.id) > 0) return Number(message.id);
  return 0;
}

async function getForumTopicInfo(client, sourceEntity, topicId) {
  const id = Number(topicId || 0);
  if (!id) return null;
  if (id === 1) return { id: 1, title: 'General', iconColor: 0x6FB9F0, iconEmojiId: null };
  try {
    const result = await client.invoke(new Api.messages.GetForumTopicsByID({
      peer: sourceEntity, topics: [id]
    }));
    const topic = result?.topics?.find(item => Number(item?.id) === id) || result?.topics?.[0];
    if (!topic) return null;
    return {
      id,
      title: String(topic.title || 'Topic ' + id).slice(0, 128),
      iconColor: Number(topic.iconColor || 0x6FB9F0),
      iconEmojiId: topic.iconEmojiId != null ? String(topic.iconEmojiId) : null
    };
  } catch {
    return null;
  }
}

async function getMappedTargetTopic(taskId, sourceTopicId) {
  const p = await db();
  const [rows] = await p.query(
    'SELECT target_topic_id FROM telegram_topic_maps WHERE task_id=? AND source_topic_id=? LIMIT 1',
    [Number(taskId), Number(sourceTopicId)]
  );
  return rows[0] ? Number(rows[0].target_topic_id) : 0;
}

async function ensureTargetForumTopic(client, task, sourceEntity, targetEntity, sourceTopicId) {
  const sid = Number(sourceTopicId || 0);
  if (!sid || sid === 1) return sid === 1 ? 1 : 0;

  const existing = await getMappedTargetTopic(task.id, sid);
  if (existing) return existing;

  const info = await getForumTopicInfo(client, sourceEntity, sid);
  if (!info) return 0;

  let created;
  try {
    created = await client.invoke(new Api.messages.CreateForumTopic({
      peer: targetEntity,
      title: info.title,
      iconColor: info.iconColor,
      iconEmojiId: info.iconEmojiId != null ? BigInt(info.iconEmojiId) : undefined,
      randomId: BigInt(Date.now()) * 1000n + BigInt(Math.floor(Math.random() * 1000))
    }));
  } catch (err) {
    if (/FORUM|TOPIC|ADMIN|WRITE_FORBIDDEN|CHANNEL_INVALID/i.test(String(err?.message || ''))) return 0;
    throw err;
  }

  let targetTopicId = 0;
  for (const update of (created?.updates || [])) {
    const msg = update?.message;
    if (msg?.action?.className === 'MessageActionTopicCreate' && Number(msg.id) > 0) {
      targetTopicId = Number(msg.id);
      break;
    }
  }
  if (!targetTopicId && created?.update?.message?.action?.className === 'MessageActionTopicCreate') {
    targetTopicId = Number(created.update.message.id || 0);
  }

  if (!targetTopicId) {
    try {
      const list = await client.invoke(new Api.channels.GetForumTopics({
        channel: targetEntity, q: info.title,
        offsetDate: 0, offsetId: 0, offsetTopic: 0, limit: 100
      }));
      const matches = (list?.topics || []).filter(item => String(item.title || '') === info.title);
      if (matches.length) {
        matches.sort((a, b) => Number(b.date || 0) - Number(a.date || 0));
        targetTopicId = Number(matches[0].id || 0);
      }
    } catch {}
  }

  if (!targetTopicId) return 0;
  const p = await db();
  await p.query(
    \`INSERT INTO telegram_topic_maps
      (task_id, source_topic_id, target_topic_id, title, icon_color, icon_emoji_id)
     VALUES (?, ?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE
       target_topic_id=VALUES(target_topic_id),
       title=VALUES(title),
       icon_color=VALUES(icon_color),
       icon_emoji_id=VALUES(icon_emoji_id),
       updated_at=CURRENT_TIMESTAMP\`,
    [Number(task.id), sid, targetTopicId, info.title, info.iconColor, info.iconEmojiId]
  );
  return targetTopicId;
}

async function getMappedTargetMessage(taskId, sourceMessageId) {
  const sid = Number(sourceMessageId || 0);
  if (!sid) return 0;
  const p = await db();
  const [rows] = await p.query(
    'SELECT target_message_id FROM forwarded_messages WHERE task_id=? AND source_message_id=? LIMIT 1',
    [Number(taskId), sid]
  );
  return rows[0] && Number(rows[0].target_message_id) > 0 ? Number(rows[0].target_message_id) : 0;
}

async function sendStructuredTelegramMessages(client, target, sourceEntity, messages, task) {
  const list = [...messages].filter(Boolean).sort((a, b) => Number(a.id) - Number(b.id));
  if (!list.length) return [];

  const groups = new Map();
  for (const msg of list) {
    const sourceTopicId = getForumTopicId(msg);
    const targetTopicId = sourceTopicId
      ? await ensureTargetForumTopic(client, task, sourceEntity, target, sourceTopicId)
      : 0;
    const key = String(targetTopicId || 0);
    if (!groups.has(key)) groups.set(key, { targetTopicId, messages: [] });
    groups.get(key).messages.push(msg);
  }

  const sent = [];
  for (const group of groups.values()) {
    const topicId = Number(group.targetTopicId || 0);
    const albums = new Map();
    const singles = [];

    for (const msg of group.messages) {
      if (msg.groupedId != null) {
        const key = String(msg.groupedId);
        if (!albums.has(key)) albums.set(key, []);
        albums.get(key).push(msg);
      } else {
        singles.push(msg);
      }
    }

    for (const msg of singles) {
      const sourceReplyId = Number(msg.replyTo?.replyToMsgId || 0);
      const mappedReplyId = sourceReplyId ? await getMappedTargetMessage(task.id, sourceReplyId) : 0;
      const replyTo = mappedReplyId || (topicId && topicId !== 1 ? topicId : undefined);
      const topMsgId = topicId && topicId !== 1 ? topicId : undefined;

      if (msg.media) {
        sent.push(await client.sendFile(target, {
          file: msg.media, caption: String(msg.message || ''), replyTo, topMsgId
        }));
      } else if (msg.message) {
        sent.push(await client.sendMessage(target, {
          message: String(msg.message), replyTo, topMsgId
        }));
      }
    }

    for (const album of albums.values()) {
      album.sort((a, b) => Number(a.id) - Number(b.id));
      const media = album.filter(msg => msg.media);
      if (!media.length) continue;
      const first = media[0];
      const sourceReplyId = Number(first.replyTo?.replyToMsgId || 0);
      const mappedReplyId = sourceReplyId ? await getMappedTargetMessage(task.id, sourceReplyId) : 0;
      const replyTo = mappedReplyId || (topicId && topicId !== 1 ? topicId : undefined);
      const topMsgId = topicId && topicId !== 1 ? topicId : undefined;

      const result = await client.sendFile(target, {
        file: media.map(msg => msg.media),
        caption: media.map(msg => String(msg.message || '')),
        replyTo, topMsgId
      });
      sent.push(...(Array.isArray(result) ? result : [result]));
    }
  }
  return sent;
}

`;
if (!s.includes(anchor)) throw new Error('找不到原消息发送函数');
s = s.replace(anchor, helper + anchor, 1);

const replacements = [
  [
    'const result = await sendTelegramMessagesWithoutSource(client, target, messages);',
    'const result = await sendStructuredTelegramMessages(client, target, source, messages, task);'
  ],
  [
    'const out=await sendTelegramMessagesWithoutSource(client,target,uniqueMessages);',
    'const out=await sendStructuredTelegramMessages(client,target,source,uniqueMessages,task);'
  ],
  [
    'const out=await sendTelegramMessagesWithoutSource(client,target,[msg]);',
    'const out=await sendStructuredTelegramMessages(client,target,source,[msg],task);'
  ]
];
for (const [a,b] of replacements) {
  if (!s.includes(a)) throw new Error('找不到预期的转发调用：' + a);
  s = s.replace(a,b);
}

fs.writeFileSync(path, s);
console.log('✅ 已修改 src/index.js：支持自动创建并克隆话题、话题图标，并保持话题内回复关系。');
try {
  execSync('node --check src/index.js', { stdio: 'inherit' });
  execSync('git add src/index.js && git commit -m "支持完整克隆话题群结构和回复关系"', { stdio: 'inherit' });
  console.log('✅ 已完成语法检查并提交本地 Git。');
} catch {
  console.log('⚠️ 文件已修改，但 Git 提交失败；请不要重复运行补丁。');
}
