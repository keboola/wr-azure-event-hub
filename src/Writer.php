<?php

declare(strict_types=1);

namespace Keboola\AzureEventHubWriter;

use Throwable;
use Keboola\AzureEventHubWriter\Configuration\Config;
use Keboola\AzureEventHubWriter\Exception\ApplicationException;
use Keboola\AzureEventHubWriter\Exception\ProcessException;
use Keboola\AzureEventHubWriter\Exception\UserException;
use Keboola\Csv\CsvReader;
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

    private LoopInterface  $loop;

    private ProcessFactory $processFactory;

    private CsvReader $csvReader;

    private array $header;

    public function __construct(LoggerInterface $logger, string $dataDir, Config $config)
    {
        $this->logger = $logger;
        $this->dataDir = $dataDir;
        $this->config = $config;
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
        $this->logger->info(sprintf('Exporting table "%s" ...', $this->config->getTableId()));
        $csvPath = $this->getCsvPath();
        $this->csvReader = new CsvReader($csvPath);

        // Get CSV header
        $header = $this->csvReader->current();
        if (!$header) {
            throw new ApplicationException(sprintf('Missing CSV header in "%s".', $csvPath));
        }
        $this->header = $header;
        $this->csvReader->next();

        // Register a new NodeJs process to event loop.
        $process = $this->createNodeJsProcess('write.js');
        $messageStream = $process->getMessageStream();
        $messageWriter = new MessageWriter($messageStream);

        // Schedule the first batch
        $this->futureWriteCsvRows($this->csvReader, $messageWriter);

        // Start event loop
        $this->loop->run();

        // Done
        $this->logger->info(sprintf(
            'Exported all %d rows from the table "%s".',
            $messageWriter->getCount(),
            $this->config->getTableId()
        ));
    }

    protected function writeCsvRows(CsvReader $csvReader, MessageWriter $messageWriter): void
    {
        if ($messageWriter->isBufferFull()) {
            usleep(5000); // wait 5ms and check again
            return;
        }

        for ($i = 0; $i < self::CSV_ROWS_BATCH_SIZE && !$messageWriter->isBufferFull(); $i++) {
            if (!$csvReader->valid()) {
                // No more rows
                $messageWriter->finish();
                return;
            }

            $message = array_combine($this->header, $csvReader->current());
            $messageWriter->writeMessage($message);
            $csvReader->next();
        }
    }

    protected function futureWriteCsvRows(CsvReader $csvReader, MessageWriter $messageWriter): void
    {
        // We write CSV lines in batches to the NodeJs file descriptor.
        // After each batch, execution returns to the loop, so that it can process other events.
        // Event loop then call "futureTick" callback.
        $this->loop->futureTick(function () use ($csvReader, $messageWriter): void {
            $this->writeCsvRows($csvReader, $messageWriter);
            if (!$messageWriter->isFinished()) {
                $this->futureWriteCsvRows($csvReader, $messageWriter);
            }
        });
    }

    protected function getCsvPath(): string
    {
        $csvPath = rtrim($this->dataDir, '/') . '/in/tables/' . $this->config->getTableCsvFile();
        if (!file_exists($csvPath)) {
            throw new ApplicationException(sprintf('CSV file "%s" not found.', $csvPath));
        }

        return $csvPath;
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
        ];
    }
}
