<?php

declare(strict_types=1);

namespace Keboola\AzureEventHubWriter\Configuration;

use Keboola\AzureEventHubWriter\Configuration\Node\HubNode;
use Keboola\Component\Config\BaseConfigDefinition;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class ConfigDefinition extends BaseConfigDefinition
{
    public const DEFAULT_BATCH_SITE = 1000;

    // Message's body is value of the configured column
    public const MODE_MESSAGE_COLUMN_VALUE = 'column_value';
    // Message's body is CSV row encoded to JSON
    public const MODE_MESSAGE_ROW_AS_JSON = 'row_as_json';

    protected function getRootDefinition(TreeBuilder $treeBuilder): ArrayNodeDefinition
    {
        $rootNode = parent::getRootDefinition($treeBuilder);

        // Check/determine tableId
        $rootNode->validate()->always(function ($v) {
            $tableId = $v['parameters']['tableId'] ?? null;
            $inputTables = array_values($v['storage']['input']['tables'] ?? []);
            if (!$tableId && count($inputTables) === 1) {
                // Get table (only one present) from the input mapping
                $v['parameters']['tableId'] = $inputTables[0]['source'];
            } elseif (!$tableId) {
                // No table found
                throw new InvalidConfigurationException(sprintf(
                    'Please define one table in the input mapping, found %d tables.',
                    count($inputTables),
                ));
            }

            return $v;
        });

        return $rootNode;
    }

    protected function getParametersDefinition(): ArrayNodeDefinition
    {
        $parametersNode = parent::getParametersDefinition();
        $parametersNode->isRequired();

        // @formatter:off
        /** @noinspection NullPointerExceptionInspection */
        $parametersNode
            ->ignoreExtraKeys(true)
            ->children()
                ->append(new HubNode())
                ->scalarNode('tableId')->cannotBeEmpty()->defaultValue(null)->end()
                ->integerNode('batchSize')->min(1)->defaultValue(self::DEFAULT_BATCH_SITE)->end()
                ->enumNode('mode')
                    ->values([self::MODE_MESSAGE_COLUMN_VALUE, self::MODE_MESSAGE_ROW_AS_JSON])
                    ->defaultValue(self::MODE_MESSAGE_ROW_AS_JSON)
                ->end()
                ->scalarNode('column')->end()
                ->scalarNode('propertiesColumn')->end()
            ->end()
        ;

        // Validation mode
        $parametersNode->validate()->always(function ($v) {
            switch ($v['mode']) {
                case self::MODE_MESSAGE_ROW_AS_JSON:
                    if (!empty($v['column'])) {
                        throw new InvalidConfigurationException(sprintf(
                            'Invalid configuration, "column" is configured, but "mode" is set to "%s".',
                            self::MODE_MESSAGE_ROW_AS_JSON,
                        ));
                    }
                    if (!empty($v['propertiesColumn'])) {
                        throw new InvalidConfigurationException(sprintf(
                            'Invalid configuration, "propertiesColumn" is configured, but "mode" is set to "%s".',
                            self::MODE_MESSAGE_ROW_AS_JSON,
                        ));
                    }
                    break;
                case self::MODE_MESSAGE_COLUMN_VALUE:
                    if (empty($v['column'])) {
                        throw new InvalidConfigurationException(sprintf(
                            'Invalid configuration, missing "column" key, "mode" is set to "%s".',
                            self::MODE_MESSAGE_COLUMN_VALUE,
                        ));
                    }
                    break;
            }

            return $v;
        });

        // @formatter:on
        return $parametersNode;
    }
}
