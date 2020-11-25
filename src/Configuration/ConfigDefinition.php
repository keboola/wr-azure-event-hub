<?php

declare(strict_types=1);

namespace Keboola\AzureEventHubWriter\Configuration;

use Keboola\AzureEventHubWriter\Configuration\Node\HubNode;
use Keboola\Component\Config\BaseConfigDefinition;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;

class ConfigDefinition extends BaseConfigDefinition
{
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
            ->end()
        ;
        // @formatter:on
        return $parametersNode;
    }
}
