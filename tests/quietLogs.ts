// Import for its side effect.
// The app logs the failures these suites provoke on purpose (SFTP refused, retries exhausted...).
// Hide just those JSON log lines so the test report stays readable; TEST_VERBOSE_LOGS=1 shows them.
if (!process.env.TEST_VERBOSE_LOGS) {
  const write = process.stdout.write.bind(process.stdout);
  process.stdout.write = ((chunk: unknown, ...rest: unknown[]) =>
    typeof chunk === "string" && chunk.startsWith('{"') && chunk.includes('"service":"sinch-ai-bot-webhook"')
      ? true
      : (write as (...a: unknown[]) => boolean)(chunk, ...rest)) as typeof process.stdout.write;
}
