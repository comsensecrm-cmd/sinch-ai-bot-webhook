/** Bull Board dashboard for the three BullMQ queues, served at BULL_BOARD_PATH. */

import { createBullBoard } from "@bull-board/api";
import { BullMQAdapter } from "@bull-board/api/bullMQAdapter";
import { ExpressAdapter } from "@bull-board/express";
import config from "../config/env";
import { dlqQueue, failureQueue, inputQueue } from "../queue/queues";

export function createBullBoardRouter() {
  const serverAdapter = new ExpressAdapter();
  serverAdapter.setBasePath(config.bullBoard.path);

  createBullBoard({
    queues: [inputQueue, failureQueue, dlqQueue].map(
      (queue) => new BullMQAdapter(queue),
    ),
    serverAdapter,
  });

  return serverAdapter.getRouter();
}
