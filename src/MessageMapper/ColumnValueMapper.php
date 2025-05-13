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

    private ?string $propertiesColumn;
    private ?int $propertiesColumnIndex = null;

    private ?string $partitionKeyColumn;
    private ?int $partitionKeyColumnIndex = null;

    private array $header;

    public function __construct(Config $config, CsvReader $csvReader)
    {
        $this->csvReader = $csvReader;
        $this->column = $config->getColumn();
        $this->propertiesColumn = $config->getPropertiesColumn();
        $this->partitionKeyColumn = $config->getPartitionKeyColumn();
        $this->header = (array) $csvReader->getHeader();

        // Get column index
        $this->columnIndex = $this->getColumnIndex($this->column, $config);

        if ($this->propertiesColumn) {
            $this->propertiesColumnIndex = $this->getColumnIndex($this->propertiesColumn, $config);
        }

        if ($this->partitionKeyColumn) {
            $this->partitionKeyColumnIndex = $this->getColumnIndex($this->partitionKeyColumn, $config);
        }

        // Skip header
        $this->csvReader->next();
    }

    public function getMessages(): Iterator
    {
        while ($this->csvReader->valid()) {
            $row = (array) $this->csvReader->current();
            $rawMessage = $row[$this->columnIndex];

            // Try convert to JSON object
            try {
                $message['message'] = json_decode($rawMessage, false, 512, JSON_THROW_ON_ERROR);
            } catch (JsonException) {
                $message['message'] = ['data' => $rawMessage];
            }

            if ($this->propertiesColumnIndex !== null) {
                $properties = $row[$this->propertiesColumnIndex] ?? null;
                try {
                    $message['properties'] = json_decode($properties, false, 512, JSON_THROW_ON_ERROR);
                } catch (JsonException) {
                    // properties must be json
                    throw new UserException(sprintf(
                        'Error decoding JSON in properties column "%s".',
                        $this->propertiesColumn,
                    ));
                }
            }

            // Add partition key if the column is specified
            if ($this->partitionKeyColumnIndex !== null) {
                $partitionKey = $row[$this->partitionKeyColumnIndex] ?? null;
                if ($partitionKey !== null && $partitionKey !== '') {
                    $message['partitionKey'] = $partitionKey;
                }
            }

            yield $message;

            $this->csvReader->next();
        }
    }

    private function getColumnIndex(string $columnName, Config $config): int
    {
        // Validate: defined column must be present in the input table
        $columnIndex = array_search($columnName, $this->header);

        if (!is_int($columnIndex)) {
            throw new UserException(sprintf(
                'Column "%s" not found in table "%s".',
                $columnName,
                $config->getTableId(),
            ));
        }

        return $columnIndex;
    }
}
