// discord.ts
//
// Direct REST wrapper over Discord's Bot API (discord.com/api/v10).
// Stateless, same shape as github.ts: every call opens its own fetch()
// with the bot token, no local state. Requires a Discord bot to be
// created and invited to the target server(s) with the relevant
// permissions (Send Messages, Manage Channels, Add Reactions, etc.),
// and the bot token stored as DISCORD_BOT_TOKEN.

const DISCORD_API = "https://discord.com/api/v10";

function headers(token: string): Record<string, string> {
  return {
    Authorization: `Bot ${token}`,
    "Content-Type": "application/json",
    "User-Agent": "turso-github-mcp (Cloudflare Worker, discord tools)",
  };
}

async function discordRequest(
  token: string,
  method: string,
  path: string,
  body?: unknown,
): Promise<{ ok: boolean; status: number; data: any }> {
  const resp = await fetch(`${DISCORD_API}${path}`, {
    method,
    headers: headers(token),
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const raw = await resp.text();
  let data: any = raw;
  try {
    data = raw ? JSON.parse(raw) : null;
  } catch {
    // leave as raw text
  }
  return { ok: resp.ok, status: resp.status, data };
}

export async function discordSendMessage(
  token: string,
  channelId: string,
  content: string,
): Promise<string> {
  const { ok, status, data } = await discordRequest(
    token,
    "POST",
    `/channels/${channelId}/messages`,
    { content },
  );
  if (!ok) return `Failed to send message: ${status} ${JSON.stringify(data)}`;
  return `Sent message ${data.id} to channel ${channelId}.`;
}

export async function discordGetChannelMessages(
  token: string,
  channelId: string,
  limit: number = 20,
): Promise<string> {
  const { ok, status, data } = await discordRequest(
    token,
    "GET",
    `/channels/${channelId}/messages?limit=${limit}`,
  );
  if (!ok) return `Failed to get messages: ${status} ${JSON.stringify(data)}`;
  const messages = (data ?? []) as any[];
  if (messages.length === 0) return "No messages found.";
  return messages
    .map((m) => `[${m.timestamp}] ${m.author?.username ?? "unknown"} (id=${m.id}): ${m.content}`)
    .join("\n");
}

export async function discordListChannels(token: string, guildId: string): Promise<string> {
  const { ok, status, data } = await discordRequest(token, "GET", `/guilds/${guildId}/channels`);
  if (!ok) return `Failed to list channels: ${status} ${JSON.stringify(data)}`;
  const channels = (data ?? []) as any[];
  if (channels.length === 0) return "No channels found.";
  return channels
    .map((c) => `id=${c.id}  type=${c.type}  name=${c.name}${c.parent_id ? `  parent=${c.parent_id}` : ""}`)
    .join("\n");
}

export async function discordCreateChannel(
  token: string,
  guildId: string,
  name: string,
  type: number = 0,
  parentId?: string,
): Promise<string> {
  const body: Record<string, unknown> = { name, type };
  if (parentId) body.parent_id = parentId;
  const { ok, status, data } = await discordRequest(token, "POST", `/guilds/${guildId}/channels`, body);
  if (!ok) return `Failed to create channel: ${status} ${JSON.stringify(data)}`;
  return `Created channel '${data.name}' (id=${data.id}, type=${data.type}) in guild ${guildId}.`;
}

export async function discordAddReaction(
  token: string,
  channelId: string,
  messageId: string,
  emoji: string,
): Promise<string> {
  // Custom emoji must be passed as name:id; unicode emoji as-is. Either
  // way it must be percent-encoded for the URL path.
  const encodedEmoji = encodeURIComponent(emoji);
  const { ok, status, data } = await discordRequest(
    token,
    "PUT",
    `/channels/${channelId}/messages/${messageId}/reactions/${encodedEmoji}/@me`,
  );
  if (!ok) return `Failed to add reaction: ${status} ${JSON.stringify(data)}`;
  return `Added reaction '${emoji}' to message ${messageId} in channel ${channelId}.`;
}

export async function discordGetGuildInfo(token: string, guildId: string): Promise<string> {
  const { ok, status, data } = await discordRequest(
    token,
    "GET",
    `/guilds/${guildId}?with_counts=true`,
  );
  if (!ok) return `Failed to get guild info: ${status} ${JSON.stringify(data)}`;
  return (
    `name=${data.name}  id=${data.id}  owner_id=${data.owner_id}  ` +
    `approximate_member_count=${data.approximate_member_count}  ` +
    `approximate_presence_count=${data.approximate_presence_count}`
  );
}
