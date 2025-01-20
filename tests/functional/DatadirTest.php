<?php

declare(strict_types=1);

namespace Keboola\AzureEventHubWriter\FunctionalTests;

use Keboola\DatadirTests\DatadirTestCase;
use Keboola\DatadirTests\DatadirTestSpecificationInterface;
use RuntimeException;
use Symfony\Component\Process\Process;

class DatadirTest extends DatadirTestCase
{
    private Process $consumerProcess;

    protected function setUp(): void
    {
        parent::setUp();

        // Remove EntityPath (alias "event hub name") from connection string if preset.
        // It is optional, and we want to test if is bad EntityPath specified.
        $connectionString = (string) getenv('CONNECTION_STRING');
        $connectionString = (string) preg_replace('~EntityPath=[^=;]+~', '', $connectionString);

        var_dump($connectionString);

        putenv("CONNECTION_STRING_NORMALIZED={$connectionString}");
        putenv("CONNECTION_STRING_BAD_ENTITY_PATH={$connectionString};EntityPath=test");
    }

    /**
     * @dataProvider provideDatadirSpecifications
     */
    public function testDatadir(DatadirTestSpecificationInterface $specification): void
    {
        $tempDir = $this->getTempDatadir($specification)->getTmpFolder();

        if ($specification->getExpectedReturnCode() === 0) {
            // Consume new event hub messages and dump them to file.
            $this->startCollectingEventHubMessages();
            $testProcess = $this->runScript($tempDir);
            $this->stopCollectionEventHubMessages($tempDir, $specification);
        } else {
            $testProcess = $this->runScript($tempDir);
        }

        $this->assertMatchesSpecification($specification, $testProcess, $tempDir);
    }

    protected function startCollectingEventHubMessages(): void
    {
        $scriptPath = __DIR__ . '/hubConsumer.js';
        $this->consumerProcess = new Process(['node', $scriptPath]);
        $this->consumerProcess->setTimeout(100.0);
        $this->consumerProcess->start();
        sleep(5);
    }

    protected function stopCollectionEventHubMessages(
        string $tempDir,
        DatadirTestSpecificationInterface $specification,
    ): void {
        // Let's wait for the messages to be delivered.
        sleep(5);

        // Check if consumer process is running
        if (!$this->consumerProcess->isRunning()) {
            throw new RuntimeException(sprintf(
                'Consumer helper process failed, STDERR=%s, STDOUT=%s',
                $this->consumerProcess->getErrorOutput(),
                $this->consumerProcess->getOutput(),
            ));
        }

        // Stop process
        $this->consumerProcess->signal(15); // TERM signal
        $this->consumerProcess->wait();
        sleep(2);

        // Dump messages if file exists in expected out
        $messages = trim($this->consumerProcess->getOutput());
        if ($messages && file_exists($specification->getExpectedOutDirectory() . '/hub_messages_dump.txt')) {
            $path = $tempDir . '/out/hub_messages_dump.txt';
            file_put_contents($path, $messages . "\n");
        }
    }
}
