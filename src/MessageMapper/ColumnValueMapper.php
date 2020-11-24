<?php

declare(strict_types=1);

namespace Keboola\AzureEventHubWriter\MessageMapper;

use Iterator;
use Keboola\AzureEventHubWriter\Configuration\Config;
use Keboola\AzureEventHubWriter\Exception\UserException;
use Keboola\Csv\CsvReader;

class ColumnValueMapper implements MessageMapper
{
    private CsvReader $csvReader;

    private string $column;

    public function __construct(Config $config, CsvReader $csvReader)
    {
        $this->csvReader = $csvReader;
        $this->column = $config->getColumn();

        // Validate: defined column must be present in the input table
        if (!in_array($this->column, $config->getTableColumns(), true)) {
            throw new UserException(sprintf(
                'Column "%s" not found in table "%s".',
                $this->column,
                $config->getTableId()
            ));
        }

        // Skip header
        $this->csvReader->next();
    }

    public function getMessages(): Iterator
    {
        while ($this->csvReader->valid()) {
            $row = $this->csvReader->current();
            yield  $row[$this->column];
            $this->csvReader->next();
        }
    }
}
