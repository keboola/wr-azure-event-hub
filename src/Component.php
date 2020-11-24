<?php

declare(strict_types=1);

namespace Keboola\AzureEventHubWriter;

use Keboola\AzureEventHubWriter\Configuration\ActionConfigDefinition;
use Keboola\AzureEventHubWriter\Configuration\Config;
use Keboola\AzureEventHubWriter\Configuration\ConfigDefinition;
use Keboola\AzureEventHubWriter\MessageMapper\MessageMapperFactory;
use Keboola\Component\BaseComponent;
use Psr\Log\LoggerInterface;

class Component extends BaseComponent
{
    public const ACTION_RUN = 'run';
    public const ACTION_TEST_CONNECTION = 'testConnection';

    private Writer $writer;

    public function __construct(LoggerInterface $logger)
    {
        parent::__construct($logger);
        $config = $this->getConfig();
        $messageMapperFactory = new MessageMapperFactory($config);
        $this->writer = new Writer($this->getLogger(), $this->getDataDir(), $config, $messageMapperFactory);
    }

    protected function run(): void
    {
        $this->writer->write();
    }

    protected function handleTestConnection(): array
    {
        $this->writer->testConnection();
        return ['success' => true];
    }

    protected function getSyncActions(): array
    {
        return [
            self::ACTION_TEST_CONNECTION => 'handleTestConnection',
        ];
    }

    public function getConfig(): Config
    {
        /** @var Config $config */
        $config = parent::getConfig();
        return $config;
    }

    protected function getConfigClass(): string
    {
        return Config::class;
    }

    protected function getConfigDefinitionClass(): string
    {
        $action = $this->getRawConfig()['action'] ?? 'run';
        return $action === 'run' ? ConfigDefinition::class : ActionConfigDefinition::class;
    }
}
