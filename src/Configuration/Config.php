<?php

declare(strict_types=1);

namespace Keboola\AzureEventHubWriter\Configuration;

use Keboola\AzureEventHubWriter\Exception\InvalidStateException;
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

    public function getTableId(): string
    {
        return $this->getValue(['parameters', 'tableId']);
    }

    public function getMode(): string
    {
        return $this->getValue(['parameters', 'mode']);
    }

    public function hasColumn(): bool
    {
        return $this->getMode() === ConfigDefinition::MODE_MESSAGE_COLUMN_VALUE;
    }

    public function getColumn(): string
    {
        if (!$this->hasColumn()) {
            throw new InvalidStateException('The "column" item is not set.');
        }

        return $this->getValue(['parameters', 'column']);
    }
}
