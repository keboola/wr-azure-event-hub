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
                'tableId' => 'in.c-ex-generic-test.data',
            ],
            [
                'connectionString' => 'Endpoint=sb://abc.servicebus.windows.net;SharedAccessKeyName=def',
                'eventHubName' => 'my-event-hub',
                'tableId' => 'in.c-ex-generic-test.data',
                'mode' => ConfigDefinition::MODE_MESSAGE_ROW_AS_JSON,
                'column' => null,
            ],
        ];

        yield 'full' => [
            [
                'hub' => $this->getHubNode(),
                'tableId' => 'in.c-ex-generic-test.data',
                'mode' => ConfigDefinition::MODE_MESSAGE_COLUMN_VALUE,
                'column' => 'foo'
            ],
            [
                'connectionString' => 'Endpoint=sb://abc.servicebus.windows.net;SharedAccessKeyName=def',
                'eventHubName' => 'my-event-hub',
                'tableId' => 'in.c-ex-generic-test.data',
                'mode' => ConfigDefinition::MODE_MESSAGE_COLUMN_VALUE,
                'column' => 'foo',
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

        yield 'column-missing' => [
            'Invalid configuration, missing "column" key, "mode" is set to "column_value".',
            [
                'hub' => $this->getHubNode(),
                'tableId' => 'in.c-ex-generic-test.data',
                'mode' => ConfigDefinition::MODE_MESSAGE_COLUMN_VALUE
            ],
        ];

        yield 'column-unexpected' => [
            'Invalid configuration, "column" is configured, but "mode" is set to "row_as_json".',
            [
                'hub' => $this->getHubNode(),
                'tableId' => 'in.c-ex-generic-test.data',
                'mode' => ConfigDefinition::MODE_MESSAGE_ROW_AS_JSON,
                'column' => 'foo'
            ],
        ];
    }

    private function configToArray(Config $config): array
    {
        return [
            'connectionString' => $config->getConnectionString(),
            'eventHubName' => $config->getEventHubName(),
            'tableId' => $config->getTableId(),
            'mode'  => $config->getMode(),
            'column' => $config->hasColumn() ? $config->getColumn() : null,
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
