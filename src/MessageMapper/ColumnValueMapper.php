<?php

declare(strict_types=1);

namespace Keboola\AzureEventHubWriter\MessageMapper;

use Iterator;
use JsonException;
use Keboola\AzureEventHubWriter\Configuration\Config;
use Keboola\AzureEventHubWriter\Exception\UserException;
use Keboola\Csv\CsvReader;

class ColumnValueMapper implements MessageMapper
{
    private CsvReader $csvReader;

    private string $column;

    private int $columnIndex;

    private array $header;

    public function __construct(Config $config, CsvReader $csvReader)
    {
        $this->csvReader = $csvReader;
        $this->column = $config->getColumn();
        $this->header = (array) $csvReader->getHeader();

        // Validate: defined column must be present in the input table
        $columnIndex = array_search($this->column, $this->header);
        if (!is_int($columnIndex)) {
            throw new UserException(sprintf(
                'Column "%s" not found in table "%s".',
                $this->column,
                $config->getTableId()
            ));
        }

        // Get column index
        $this->columnIndex = $columnIndex;

        // Skip header
        $this->csvReader->next();
    }

    public function getMessages(): Iterator
    {
        while ($this->csvReader->valid()) {
            $row = $this->csvReader->current();
            $rawMessage = $row[$this->columnIndex];

            // Try convert to JSON object
            try {
                yield json_decode($rawMessage, false, 512, JSON_THROW_ON_ERROR);
            } catch (JsonException $e) {
                yield ['data' => $rawMessage];
            }

            $this->csvReader->next();
        }
    }
}
