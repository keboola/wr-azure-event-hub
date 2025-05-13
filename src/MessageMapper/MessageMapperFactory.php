<?php

declare(strict_types=1);

namespace Keboola\AzureEventHubWriter\MessageMapper;

use Keboola\AzureEventHubWriter\Configuration\Config;
use Keboola\AzureEventHubWriter\Configuration\ConfigDefinition;
use Keboola\AzureEventHubWriter\Exception\ApplicationException;
use Keboola\AzureEventHubWriter\Exception\UnexpectedValueException;
use Keboola\Csv\CsvReader;

class MessageMapperFactory
{
    private Config $config;

    private string $dataDir;

    public function __construct(Config $config, string $dataDir)
    {
        $this->config = $config;
        $this->dataDir = $dataDir;
    }

    public function create(): MessageMapper
    {
        $csvReader = $this->createCsvReader();

        switch ($this->config->getMode()) {
            case ConfigDefinition::MODE_MESSAGE_ROW_AS_JSON:
                return new RowAsJsonMapper($csvReader, $this->config->getPartitionKeyColumn());
            case ConfigDefinition::MODE_MESSAGE_COLUMN_VALUE:
                return new ColumnValueMapper($this->config, $csvReader);
            default:
                throw new UnexpectedValueException(sprintf('Unexpected mode "%s".', $this->config->getMode()));
        }
    }

    private function createCsvReader(): CsvReader
    {
        return new CsvReader($this->getCsvPath());
    }

    private function getCsvPath(): string
    {
        $csvPath = rtrim($this->dataDir, '/') . '/in/tables/' . $this->config->getTableCsvFile();
        if (!file_exists($csvPath)) {
            throw new ApplicationException(sprintf('CSV file "%s" not found.', $csvPath));
        }

        return $csvPath;
    }
}
