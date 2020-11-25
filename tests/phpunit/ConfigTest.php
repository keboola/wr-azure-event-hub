<?php

declare(strict_types=1);

namespace Keboola\AzureEventHubWriter\Tests;

use Keboola\AzureEventHubWriter\Configuration\Config;
use Keboola\AzureEventHubWriter\Configuration\ConfigDefinition;
use PHPUnit\Framework\Assert;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class ConfigTest extends AbstractTestCase
{
    /**
     * @dataProvider getValidConfigs
     */
    public function testValidConfig(array $input, array $expected): void
    {
        $config = new Config(['parameters' => $input], new ConfigDefinition());
        Assert::assertSame($expected, $this->configToArray($config));
    }

    /**
     * @dataProvider getInvalidConfigs
     */
    public function testInvalidConfig(string $expectedMsg, array $input): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectDeprecationMessage($expectedMsg);
        new Config(['parameters' => $input], new ConfigDefinition());
    }

    public function getValidConfigs(): iterable
    {
        yield 'minimal' => [
            [
                'hub' => $this->getHubNode(),
            ],
            [
                'connectionString' => 'Endpoint=sb://abc.servicebus.windows.net;SharedAccessKeyName=def',
                'eventHubName' => 'my-event-hub',
            ],
        ];
    }

    public function getInvalidConfigs(): iterable
    {
        yield 'empty' => [
            'The child node "hub" at path "root.parameters" must be configured.',
            [],
        ];

        yield 'empty-hub' => [
            'The child node "#connectionString" at path "root.parameters.hub" must be configured.',
            [
                'hub' => [],
            ],
        ];
    }

    private function configToArray(Config $config): array
    {
        return [
            'connectionString' => $config->getConnectionString(),
            'eventHubName' => $config->getEventHubName(),
        ];
    }

    private function getHubNode(): array
    {
        return [
            '#connectionString' => 'Endpoint=sb://abc.servicebus.windows.net;SharedAccessKeyName=def',
            'eventHubName' => 'my-event-hub',
        ];
    }
}
