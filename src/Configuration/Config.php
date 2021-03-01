<?php

declare(strict_types=1);

namespace Keboola\AzureEventHubWriter\Configuration;

use Keboola\AzureEventHubWriter\Exception\InvalidStateException;
use Keboola\AzureEventHubWriter\Exception\UserException;
use Keboola\Component\Config\BaseConfig;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class Config extends BaseConfig
{
    public function getConnectionString(): string
    {
        $connectionString = $this->getValue(['parameters', 'hub', '#connectionString'], false);
        $imageParams = $this->getImageParameters();
        if (isset($imageParams['hub']['#connectionString'])) {
            $connectionString = $imageParams['hub']['#connectionString'];
        }
        if (!$connectionString) {
            throw new InvalidConfigurationException(
                'The child node "#connectionString" at path "root.parameters.hub" must be configured.'
            );
        }
        return $connectionString;
    }

    public function getEventHubName(): string
    {
        return $this->getValue(['parameters', 'hub', 'eventHubName']);
    }

    public function getTableId(): string
    {
        return $this->getValue(['parameters', 'tableId']);
    }

    public function getTable(): array
    {
        $tableId = $this->getTableId();
        foreach ($this->getInputTables() as $table) {
            if ($table['source'] === $tableId) {
                return $table;
            }
        }

        throw new UserException(sprintf('Table source = "%s" not found in the input mapping.', $tableId));
    }

    public function getTableCsvFile(): string
    {
        return $this->getTable()['destination'];
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

    public function getBatchSize(): int
    {
        return $this->getValue(['parameters', 'batchSize']);
    }
}
