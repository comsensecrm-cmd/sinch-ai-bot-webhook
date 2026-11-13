// Preloaded for every test file (see the "test" script). dotenv never overrides a variable that is
// already set, so blanking these keeps a developer's .env from sending test logs, including the
// failures tests provoke on purpose, to the real CloudWatch group. Also keeps tests off the real
// SFTP server.
for (const name of [
  "AWS_LOG_GROUP",
  "AWS_LOG_STREAM",
  "SFTP_HOST",
  "SFTP_USERNAME",
]) {
  process.env[name] = "";
}
