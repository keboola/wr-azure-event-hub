<?php

declare(strict_types=1);

namespace Keboola\AzureEventHubWriter\MessageMapper;

use Iterator;
use Keboola\AzureEventHubWriter\Exception\ApplicationException;
use Keboola\Csv\CsvReader;

class RowAsJsonMapper implements MessageMapper
{
    private CsvReader $csvReader;

    private array $header;

    public function __construct(CsvReader $csvReader)
    {
        $this->csvReader = $csvReader;

        // Get header
        $header = $this->csvReader->current();
        if (!$header) {
            throw new ApplicationException(sprintf('Missing CSV header.'));
        }
        $this->header = (array) $header;

        // Skip header
        $this->csvReader->next();
    }

    public function getMessages(): Iterator
    {
        while ($this->csvReader->valid()) {
            yield  ['message' => array_combine($this->header, (array) $this->csvReader->current())];
            $this->csvReader->next();
        }
    }
}
