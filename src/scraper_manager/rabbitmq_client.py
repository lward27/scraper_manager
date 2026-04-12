"""RabbitMQ client primitives for queue publishing and consumption."""

import json
from typing import Optional

import aio_pika
from aio_pika.abc import AbstractIncomingMessage

from scraper_manager.config import Config
from scraper_manager.logger import get_logger

log = get_logger(__name__)


class RabbitMQClient:
    def __init__(self, config: Config):
        self.config = config
        self.connection: Optional[aio_pika.RobustConnection] = None
        self.channel: Optional[aio_pika.abc.AbstractChannel] = None
        self.work_queue = None

    async def connect(self) -> None:
        self.connection = await aio_pika.connect_robust(self.config.services.rabbitmq_url)
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=self.config.queue.prefetch_count)

        self.work_queue = await self.channel.declare_queue(
            self.config.queue.work_queue,
            durable=True,
        )
        await self.channel.declare_queue(
            self.config.queue.retry_queue,
            durable=True,
            arguments={
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": self.config.queue.work_queue,
            },
        )
        await self.channel.declare_queue(
            self.config.queue.dlq_queue,
            durable=True,
        )

        log.logger.info("Connected to RabbitMQ and declared queues")

    async def close(self) -> None:
        if self.connection:
            await self.connection.close()
            log.logger.info("RabbitMQ connection closed")

    async def _publish(
        self,
        routing_key: str,
        payload: dict,
        expiration_ms: Optional[int] = None,
        headers: Optional[dict] = None,
    ) -> None:
        if self.channel is None:
            raise RuntimeError("RabbitMQClient not connected")

        message = aio_pika.Message(
            body=json.dumps(payload).encode("utf-8"),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            content_type="application/json",
            expiration=str(expiration_ms) if expiration_ms is not None else None,
            headers=headers or {},
        )

        await self.channel.default_exchange.publish(message, routing_key=routing_key)

    async def publish_task(self, payload: dict) -> None:
        await self._publish(self.config.queue.work_queue, payload)

    async def publish_retry(self, payload: dict, delay_seconds: float, error: str) -> None:
        await self._publish(
            self.config.queue.retry_queue,
            payload,
            expiration_ms=max(1, int(delay_seconds * 1000)),
            headers={"retry_error": error},
        )

    async def publish_dlq(self, payload: dict, error: str) -> None:
        wrapped = {
            "error": error,
            "payload": payload,
        }
        await self._publish(self.config.queue.dlq_queue, wrapped)

    async def consume(self, handler):
        if self.work_queue is None:
            raise RuntimeError("RabbitMQClient not connected")
        return await self.work_queue.consume(handler, no_ack=False)

    async def cancel_consumer(self, consumer_tag: str) -> None:
        if self.work_queue is not None:
            await self.work_queue.cancel(consumer_tag)
