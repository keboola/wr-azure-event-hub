<?php

declare(strict_types=1);

namespace Keboola\AzureEventHubWriter\Configuration;

use Keboola\AzureEventHubWriter\Configuration\Node\HubNode;
use Keboola\Component\Config\BaseConfigDefinition;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class ConfigDefinition extends BaseConfigDefinition
{
    // Message content is value of the configured column
    public const MODE_MESSAGE_COLUMN_VALUE = 'column_value';
    // Message content is JSON encoded CSV row
    public const MODE_MESSAGE_ROW_AS_JSON = 'row_as_json';

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
                ->scalarNode('tableId')->isRequired()->cannotBeEmpty()->end()
                ->enumNode('mode')
                    ->values([self::MODE_MESSAGE_COLUMN_VALUE, self::MODE_MESSAGE_ROW_AS_JSON])
                    ->defaultValue(self::MODE_MESSAGE_ROW_AS_JSON)
                ->end()
                ->scalarNode('column')->end()
            ->end()
        ;

        // Validation
        $parametersNode->validate()->always(function ($v) {
            // Validate mode
            switch ($v['mode']) {
                case self::MODE_MESSAGE_ROW_AS_JSON:
                    if (!empty($v['column'])) {
                        throw new InvalidConfigurationException(sprintf(
                            'Invalid configuration, "column" is configured, but "mode" is set to "%s".',
                            self::MODE_MESSAGE_ROW_AS_JSON
                        ));
                    }
                    break;
                case self::MODE_MESSAGE_COLUMN_VALUE:
                    if (empty($v['column'])) {
                        throw new InvalidConfigurationException(sprintf(
                            'Invalid configuration, missing "column" key, "mode" is set to "%s".',
                            self::MODE_MESSAGE_COLUMN_VALUE
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
