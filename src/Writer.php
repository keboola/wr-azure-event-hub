<?php

declare(strict_types=1);

namespace Keboola\AzureEventHubWriter;

use Throwable;
use Iterator;
use Keboola\AzureEventHubWriter\MessageMapper\MessageMapperFactory;
use Keboola\AzureEventHubWriter\Configuration\Config;
use Keboola\AzureEventHubWriter\Exception\ApplicationException;
use Keboola\AzureEventHubWriter\Exception\ProcessException;
use Keboola\AzureEventHubWriter\Exception\UserException;
use Psr\Log\LoggerInterface;
use React\EventLoop\Factory as EventLoopFactory;
use React\EventLoop\LoopInterface;

class Writer
{
    public const LOG_PROGRESS_SECONDS = 30;
    public const CSV_ROWS_BATCH_SIZE = 10;

    private LoggerInterface $logger;

    private string $dataDir;

    private Config $config;

    private MessageMapperFactory $messageMapperFactory;

    private LoopInterface  $loop;

    private ProcessFactory $processFactory;

    public function __construct(
        LoggerInterface $logger,
        string $dataDir,
        Config $config,
        MessageMapperFactory $messageMapperFactory
    ) {
        $this->logger = $logger;
        $this->dataDir = $dataDir;
        $this->config = $config;
        $this->messageMapperFactory = $messageMapperFactory;
        $this->loop = EventLoopFactory::create();
        $this->processFactory = new ProcessFactory($this->logger, $this->loop);
    }

    public function testConnection(): void
    {
        // Register a new NodeJs process to event loop.
        $process = $this->createNodeJsProcess('testConnection.js');

        // On sync actions are logged only errors (no info/warning messages)
        // ... because on sync action success -> JSON output is expected.
        // So we need to capture STDERR and wrap it in an exception on process failure.
        $stderr = '';
        $process->getStderr()->on('data', function (string $chunk) use (&$stderr): void {
            $stderr .= $chunk;
        });

        // Convert process failure to User/Application exception
        $process
            ->getPromise()
            ->done(null, function (Throwable $e) use (&$stderr): void {
                $msg = trim($stderr ?: $e->getMessage());
                if ($e instanceof ProcessException && $e->getExitCode() === 1) {
                    throw new UserException($msg, $e->getCode(), $e);
                } else {
                    throw new ApplicationException($msg, $e->getCode(), $e);
                }
            });

        // Start event loop
        $this->loop->run();
    }

    public function write(): void
    {
        // Create CSV reader
        $this->logger->info(sprintf(
            'Exporting table "%s" in "%s" mode ...',
            $this->config->getTableId(),
            $this->config->getMode()
        ));

        // Create mapper
        $mapper = $this->messageMapperFactory->create();

        // Register a new NodeJs process to event loop.
        $process = $this->createNodeJsProcess('write.js');
        $messageStream = $process->getMessageStream();
        $messageWriter = new MessageWriter($messageStream);

        // Throw an exception on process failure
        $process
            ->getPromise()
            ->done(null, function (Throwable $e): void {
                if ($e instanceof ProcessException && $e->getExitCode() === 1) {
                    throw new UserException('Export failed.', $e->getCode(), $e);
                } else {
                    throw new ApplicationException($e->getMessage(), $e->getCode(), $e);
                }
            });

        // Schedule the first batch
        $this->futureWriteCsvRows($mapper->getMessages(), $messageWriter);

        // Start event loop
        $this->loop->run();

        // Done
        $this->logger->info(sprintf(
            'Exported all %d rows from the table "%s".',
            $messageWriter->getCount(),
            $this->config->getTableId()
        ));
    }

    protected function writeCsvRows(Iterator $messages, MessageWriter $messageWriter): void
    {
        if ($messageWriter->isBufferFull()) {
            usleep(5000); // wait 5ms and check again
            return;
        }

        for ($i = 0; $i < self::CSV_ROWS_BATCH_SIZE && !$messageWriter->isBufferFull(); $i++) {
            if (!$messages->valid()) {
                // No more rows
                $messageWriter->finish();
                return;
            }

            $messageWriter->writeMessage($messages->current());
            $messages->next();
        }
    }

    protected function futureWriteCsvRows(Iterator $messages, MessageWriter $messageWriter): void
    {
        // We write CSV lines in batches to the NodeJs file descriptor.
        // After each batch, execution returns to the loop, so that it can process other events.
        // Event loop then call "futureTick" callback.
        $this->loop->futureTick(function () use ($messages, $messageWriter): void {
            $this->writeCsvRows($messages, $messageWriter);
            if (!$messageWriter->isFinished()) {
                $this->futureWriteCsvRows($messages, $messageWriter);
            }
        });
    }

    protected function createNodeJsProcess(string $script): ProcessWrapper
    {
        return $this->processFactory->create(sprintf('node %s/NodeJs/%s', __DIR__, $script), $this->getProcessEnv());
    }

    protected function getProcessEnv(): array
    {
        return [
            'MESSAGE_DELIMITER' => json_encode(MessageWriter::DELIMITER),
            'CONNECTION_STRING' => $this->config->getConnectionString(),
            'EVENT_HUB_NAME' => $this->config->getEventHubName(),
            'BATCH_SIZE' => $this->config->getAction() === 'run' ? $this->config->getBatchSize() : null,
        ];
    }
}
