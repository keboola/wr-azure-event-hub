<?php

declare(strict_types=1);

namespace Keboola\AzureEventHubWriter\Tests;

use Keboola\AzureEventHubWriter\ProcessFactory;
use Keboola\AzureEventHubWriter\ProcessWrapper;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;

abstract class AbstractTestCase extends TestCase
{
    protected Logger $logger;
    protected TestHandler $loggerTestHandler;
    protected LoopInterface $loop;
    protected ProcessFactory $processFactory;

    protected function setUp(): void
    {
        parent::setUp();
        $this->loggerTestHandler = new TestHandler();
        $this->logger = new Logger('test', [$this->loggerTestHandler]);
        $this->loop = Loop::get();
        $this->processFactory = new ProcessFactory($this->logger, $this->loop);
    }

    protected function createScriptProcess(string $script): ProcessWrapper
    {
        return $this->processFactory->create(sprintf('node %s/fixtures/%s', __DIR__, $script));
    }
}
