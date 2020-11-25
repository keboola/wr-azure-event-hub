<?php

declare(strict_types=1);

namespace Keboola\AzureEventHubWriter\Tests;

use Keboola\AzureEventHubWriter\ProcessFactory;
use Keboola\AzureEventHubWriter\ProcessWrapper;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;
use React\EventLoop\Factory;
use React\EventLoop\LoopInterface;

abstract class AbstractTestCase extends TestCase
{
    protected TestLogger $logger;

    protected LoopInterface $loop;

    protected ProcessFactory $processFactory;

    protected function setUp(): void
    {
        parent::setUp();
        $this->logger = new TestLogger();
        $this->loop = Factory::create();
        $this->processFactory = new ProcessFactory($this->logger, $this->loop);
    }

    protected function createScriptProcess(string $script): ProcessWrapper
    {
        return $this->processFactory->create(sprintf('node %s/fixtures/%s', __DIR__, $script));
    }
}
