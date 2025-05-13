<?php

declare(strict_types=1);

namespace Keboola\AzureEventHubWriter\MessageMapper;

use Iterator;
use Keboola\AzureEventHubWriter\Exception\ApplicationException;
use Keboola\AzureEventHubWriter\Exception\UserException;
use Keboola\Csv\CsvReader;

class RowAsJsonMapper implements MessageMapper
{
    private CsvReader $csvReader;

    private array $header;
    
    private ?string $partitionKeyColumn;
    
    private ?int $partitionKeyColumnIndex = null;

    public function __construct(CsvReader $csvReader, ?string $partitionKeyColumn = null)
    {
        $this->csvReader = $csvReader;
        $this->partitionKeyColumn = $partitionKeyColumn;

        // Get header
        $header = $this->csvReader->current();
        if (!$header) {
            throw new ApplicationException(sprintf('Missing CSV header.'));
        }
        $this->header = (array) $header;
        
        // Get partition key column index if specified
        if ($this->partitionKeyColumn) {
            $this->partitionKeyColumnIndex = $this->getColumnIndex($this->partitionKeyColumn);
        }

        // Skip header
        $this->csvReader->next();
    }

    public function getMessages(): Iterator
    {
        while ($this->csvReader->valid()) {
            $row = (array) $this->csvReader->current();
            $message = ['message' => array_combine($this->header, $row)];
            
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
    
    private function getColumnIndex(string $columnName): int
    {
        $columnIndex = array_search($columnName, $this->header);

        if ($columnIndex === false) {
            throw new UserException(sprintf(
                'Partition key column "%s" not found in the table.',
                $columnName
            ));
        }

        return $columnIndex;
    }
}
