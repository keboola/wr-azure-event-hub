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
    public function testValidConfig(array $configArray, array $expected): void
    {
        $config = new Config($configArray, new ConfigDefinition());
        Assert::assertSame($expected, $this->configToArray($config));
    }

    /**
     * @dataProvider getInvalidConfigs
     */
    public function testInvalidConfig(string $expectedMsg, array $configArray): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectDeprecationMessage($expectedMsg);
        new Config($configArray, new ConfigDefinition());
    }

    public function testMissingConnectionString(): void
    {
        $configArray = [
            'parameters' => [
                'hub' => [
                    'eventHubName' => $this->getHubNode()['eventHubName'],
                ],
                'tableId' => 'in.c-ex-generic-test.data',
            ],
        ];
        $config = new Config($configArray, new ConfigDefinition());

        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage(
            'The child node "#connectionString" at path "root.parameters.hub" must be configured.'
        );
        $config->getConnectionString();
    }

    public function getValidConfigs(): iterable
    {
        yield 'minimal' => [
            [
                'parameters' => [
                    'hub' => $this->getHubNode(),
                    'tableId' => 'in.c-ex-generic-test.data',
                ],
            ],
            [
                'connectionString' => 'Endpoint=sb://abc.servicebus.windows.net;SharedAccessKeyName=def',
                'eventHubName' => 'my-event-hub',
                'tableId' => 'in.c-ex-generic-test.data',
                'batchSize' => ConfigDefinition::DEFAULT_BATCH_SITE,
                'mode' => ConfigDefinition::MODE_MESSAGE_ROW_AS_JSON,
                'column' => null,
            ],
        ];

        yield 'batch-size' => [
            [
                'parameters' => [
                    'hub' => $this->getHubNode(),
                    'tableId' => 'in.c-ex-generic-test.data',
                    'batchSize' => 123,
                ],
            ],
            [
                'connectionString' => 'Endpoint=sb://abc.servicebus.windows.net;SharedAccessKeyName=def',
                'eventHubName' => 'my-event-hub',
                'tableId' => 'in.c-ex-generic-test.data',
                'batchSize' => 123,
                'mode' => ConfigDefinition::MODE_MESSAGE_ROW_AS_JSON,
                'column' => null,
            ],
        ];

        yield 'table-id-from-input-mapping' => [
            [
                'storage' => [
                    'input' => [
                        'tables' => [
                            [
                                'source' => 'in.c-ex-generic-test.data',
                                'destination' => 'data.csv',
                            ],
                        ],
                    ],
                ],
                'parameters' => [
                    'hub' => $this->getHubNode(),
                ],
            ],
            [
                'connectionString' => 'Endpoint=sb://abc.servicebus.windows.net;SharedAccessKeyName=def',
                'eventHubName' => 'my-event-hub',
                'tableId' => 'in.c-ex-generic-test.data',
                'batchSize' => ConfigDefinition::DEFAULT_BATCH_SITE,
                'mode' => ConfigDefinition::MODE_MESSAGE_ROW_AS_JSON,
                'column' => null,
            ],
        ];

        yield 'full' => [
            [
                'parameters' => [
                    'hub' => $this->getHubNode(),
                    'tableId' => 'in.c-ex-generic-test.data',
                    'mode' => ConfigDefinition::MODE_MESSAGE_COLUMN_VALUE,
                    'column' => 'foo',
                ],
            ],
            [
                'connectionString' => 'Endpoint=sb://abc.servicebus.windows.net;SharedAccessKeyName=def',
                'eventHubName' => 'my-event-hub',
                'tableId' => 'in.c-ex-generic-test.data',
                'batchSize' => ConfigDefinition::DEFAULT_BATCH_SITE,
                'mode' => ConfigDefinition::MODE_MESSAGE_COLUMN_VALUE,
                'column' => 'foo',
            ],
        ];

        yield 'connection-string-in-parameters' => [
            [
                'parameters' => [
                    'hub' => $this->getHubNode(),
                    'tableId' => 'in.c-ex-generic-test.data',
                ],
            ],
            [
                'connectionString' => 'Endpoint=sb://abc.servicebus.windows.net;SharedAccessKeyName=def',
                'eventHubName' => 'my-event-hub',
                'tableId' => 'in.c-ex-generic-test.data',
                'batchSize' => ConfigDefinition::DEFAULT_BATCH_SITE,
                'mode' => ConfigDefinition::MODE_MESSAGE_ROW_AS_JSON,
                'column' => null,
            ],
        ];

        yield 'connection-string-in-image-parameters' => [
            [
                'parameters' => [
                    'hub' => [
                        'eventHubName' => $this->getHubNode()['eventHubName'],
                    ],
                    'tableId' => 'in.c-ex-generic-test.data',
                ],
                'image_parameters' => [
                    'hub' => [
                        '#connectionString' => $this->getHubNode()['#connectionString'],
                    ],
                ],
            ],
            [
                'connectionString' => 'Endpoint=sb://abc.servicebus.windows.net;SharedAccessKeyName=def',
                'eventHubName' => 'my-event-hub',
                'tableId' => 'in.c-ex-generic-test.data',
                'batchSize' => ConfigDefinition::DEFAULT_BATCH_SITE,
                'mode' => ConfigDefinition::MODE_MESSAGE_ROW_AS_JSON,
                'column' => null,
            ],
        ];

        yield 'connection-string-in-both' => [
            [
                'parameters' => [
                    'hub' => $this->getHubNode(),
                    'tableId' => 'in.c-ex-generic-test.data',
                ],
                'image_parameters' => [
                    'hub' => [
                        '#connectionString' => 'Endpoint=sb://abc.servicebus.windows.net;SharedAccessKeyName=fromImg',
                    ],
                ],
            ],
            [
                'connectionString' => 'Endpoint=sb://abc.servicebus.windows.net;SharedAccessKeyName=fromImg',
                'eventHubName' => 'my-event-hub',
                'tableId' => 'in.c-ex-generic-test.data',
                'batchSize' => ConfigDefinition::DEFAULT_BATCH_SITE,
                'mode' => ConfigDefinition::MODE_MESSAGE_ROW_AS_JSON,
                'column' => null,
            ],
        ];
    }

    public function getInvalidConfigs(): iterable
    {
        yield 'empty' => [
            'The child node "hub" at path "root.parameters" must be configured.',
            [
                'parameters' => [],
            ],
        ];

        yield 'empty-hub' => [
            'The child node "eventHubName" at path "root.parameters.hub" must be configured.',
            [
                'parameters' => [
                    'hub' => [],
                ],
            ],
        ];

        yield 'table-id-missing' => [
            'Please define one table in the input mapping, found 0 tables.',
            [
                'parameters' => [
                    'hub' => $this->getHubNode(),
                ],
            ],
        ];

        yield 'multiple-input-tables' => [
            'Please define one table in the input mapping, found 2 tables.',
            [
                'storage' => [
                    'input' => [
                        'tables' => [
                            [
                                'source' => 'a',
                                'destination' => 'a.csv',
                            ],
                            [
                                'source' => 'b',
                                'destination' => 'b.csv',
                            ],
                        ],
                    ],
                ],
                'parameters' => [
                    'hub' => $this->getHubNode(),
                ],
            ],
        ];

        yield 'column-missing' => [
            'Invalid configuration, missing "column" key, "mode" is set to "column_value".',
            [
                'parameters' => [
                    'hub' => $this->getHubNode(),
                    'tableId' => 'in.c-ex-generic-test.data',
                    'mode' => ConfigDefinition::MODE_MESSAGE_COLUMN_VALUE,
                ],
            ],
        ];

        yield 'column-unexpected' => [
            'Invalid configuration, "column" is configured, but "mode" is set to "row_as_json".',
            [
                'parameters' => [
                    'hub' => $this->getHubNode(),
                    'tableId' => 'in.c-ex-generic-test.data',
                    'mode' => ConfigDefinition::MODE_MESSAGE_ROW_AS_JSON,
                    'column' => 'foo',
                ],
            ],
        ];
    }


    private function configToArray(Config $config): array
    {
        return [
            'connectionString' => $config->getConnectionString(),
            'eventHubName' => $config->getEventHubName(),
            'tableId' => $config->getTableId(),
            'batchSize' => $config->getBatchSize(),
            'mode' => $config->getMode(),
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
