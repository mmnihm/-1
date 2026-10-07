import 'dotenv/config';
import readline from 'node:readline/promises';
import { stdin as input, stdout as output } from 'node:process';
import { TelegramClient } from 'telegram';
import { StringSession } from 'telegram/sessions/index.js';

const rl = readline.createInterface({ input, output });

try {
  const apiIdText = process.env.TG_API_ID || await rl.question('Telegram API ID: ');
  const apiHash = process.env.TG_API_HASH || await rl.question('Telegram API Hash: ');
  if (!apiIdText || !apiHash) throw new Error('缺少 TG_API_ID 或 TG_API_HASH');

  const client = new TelegramClient(
    new StringSession(''),
    Number(apiIdText),
    apiHash,
    { connectionRetries: 5 }
  );

  await client.start({
    phoneNumber: async () => await rl.question('Telegram 手机号（含国家区号）: '),
    phoneCode: async () => await rl.question('Telegram 验证码: '),
    password: async () => await rl.question('Telegram 两步验证密码（如没有直接回车）: '),
    onError: err => console.error('登录错误：', err?.message || err)
  });

  console.log('\n登录成功。请把下面这一整行保存到 VPS 的 .env：\n');
  console.log('TG_SESSION=' + client.session.save());
  console.log('\n然后重启机器人即可。不要把 TG_SESSION 发给别人。');
  await client.disconnect();
} finally {
  rl.close();
}
