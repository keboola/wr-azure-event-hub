<?php

declare(strict_types=1);

namespace Keboola\AzureEventHubWriter;

use Generator;
use Keboola\AzureEventHubWriter\Exception\ProcessException;
use Psr\Log\LoggerInterface;
use React\ChildProcess\Process;
use React\EventLoop\LoopInterface;
use React\Promise\Deferred;

class ProcessFactory
{
    public const MESSAGE_STREAM_FD = 3;

    private LoggerInterface $logger;

    private LoopInterface $loop;

    public function __construct(LoggerInterface $logger, LoopInterface $loop)
    {
        $this->logger = $logger;
        $this->loop = $loop;
    }

    public function create(string $cmd, array $env = []): ProcessWrapper
    {
        $fileDescriptors = [
            // STDIN
            0 => ['pipe', 'r'],
            // STDOUT
            1 => ['pipe', 'w'],
            // STDERR
            2 => ['pipe', 'w'],
            // MESSAGE STREAM (custom)
            self::MESSAGE_STREAM_FD => ['pipe', 'r'],
        ];

        // Let NodeJs script know which file descriptor should be used to write JSON documents to
        $env['MESSAGE_STREAM_FD'] = self::MESSAGE_STREAM_FD;

        // Create process and attach it to the event loop
        $process = new Process($cmd, null, $env, $fileDescriptors);
        $process->start($this->loop);

        // Log process stdout output as info
        $process->stdout->on('data', function (string $chunk): void {
            foreach ($this->explodeLines($chunk) as $line) {
                $this->logger->info($line);
            }
        });

        // Log process stderr output as warning
        $process->stderr->on('data', function (string $chunk): void {
            foreach ($this->explodeLines($chunk) as $line) {
                $this->logger->warning((string) $line);
            }
        });

        // Handle process exit
        $deferred = new Deferred();
        $process->on('exit', function (int $exitCode) use ($cmd, $deferred): void {
            if ($exitCode === 0) {
                $this->logger->debug(sprintf('Process "%s" completed successfully.', $cmd));
                $deferred->resolve();
            } else {
                $deferred->reject(
                    new ProcessException(sprintf('Process "%s" exited with code "%d".', $cmd, $exitCode), $exitCode),
                );
            }

            // Make sure the event loop ends
            $this->loop->stop();
        });

        return new ProcessWrapper($process, $deferred->promise());
    }

    /**
     * @return Generator<string>
     */
    private function explodeLines(string $str): Generator
    {
        foreach (explode("\n", $str) as $line) {
            $line = trim($line);
            if ($line) {
                yield $line;
            }
        }
    }
}
