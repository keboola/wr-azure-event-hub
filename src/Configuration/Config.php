<?php

declare(strict_types=1);

namespace Keboola\AzureEventHubWriter\Configuration;

use Keboola\Component\Config\BaseConfig;

class Config extends BaseConfig
{
    public function getConnectionString(): string
    {
        return $this->getValue(['parameters', 'hub', '#connectionString']);
    }

    public function getEventHubName(): string
    {
        return $this->getValue(['parameters', 'hub', 'eventHubName']);
    }
}
