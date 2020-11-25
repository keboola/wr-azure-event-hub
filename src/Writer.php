<?php

declare(strict_types=1);

namespace Keboola\AzureEventHubWriter;

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

    private LoggerInterface $logger;

    private string $dataDir;

    private Config $config;

    private LoopInterface  $loop;

    private ProcessFactory $processFactory;

    private int $processed;

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
        $process = $this->createNodeJsProcess('testConnection.js', $this->getTestConnectionEnv());

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
            ->done(null, function (\Throwable $e) use (&$stderr): void {
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
        // TODO
    }

    protected function createNodeJsProcess(string $script, array $env): ProcessWrapper
    {
        return $this->processFactory->create(sprintf('node %s/NodeJs/%s', __DIR__, $script), $env);
    }

    protected function getTestConnectionEnv(): array
    {
        return [
            'JSON_DELIMITER' => json_encode(MessageSerializer::DELIMITER),
            'CONNECTION_STRING' => $this->config->getConnectionString(),
            'EVENT_HUB_NAME' => $this->config->getEventHubName(),
        ];
    }
}
