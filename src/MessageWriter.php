<?php

declare(strict_types=1);

namespace Keboola\AzureEventHubWriter;

use Keboola\AzureEventHubWriter\Exception\ApplicationException;
use React\Stream\WritableStreamInterface;
use Throwable;

class MessageWriter
{
    public const DELIMITER = "\n---\n";

    private WritableStreamInterface $messageStream;

    private bool $bufferFull = false;

    private bool $finished = false;

    private int $processed = 0;

    public function __construct(WritableStreamInterface $messageStream)
    {
        $this->messageStream = $messageStream;
        $this->messageStream->on('error', function (Throwable $e): void {
            throw new ApplicationException($e->getMessage(), $e->getCode(), $e);
        });
        $this->messageStream->on('drain', function (): void {
            $this->bufferFull = false;
        });
    }

    public function isBufferFull(): bool
    {
        return $this->bufferFull;
    }

    public function isFinished(): bool
    {
        return $this->finished;
    }

    public function getCount(): int
    {
        return $this->processed;
    }

    /**
     * @param mixed $message
     */
    public function writeMessage($message): void
    {
        $this->write((string) json_encode($message, JSON_THROW_ON_ERROR));
        $this->write(self::DELIMITER);
        $this->processed++;
    }

    public function finish(): void
    {
        $this->finished = true;
        $this->messageStream->end();
    }

    private function write(string $data): void
    {
        $this->bufferFull = !$this->messageStream->write($data);
    }
}
