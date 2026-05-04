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
        $rowNumber = 0;
        $expectedColumnCount = count($this->header);
        while ($this->csvReader->valid()) {
            $rowNumber++;
            $row = (array) $this->csvReader->current();
            RowColumnCountValidator::assertMatchesHeader($rowNumber, count($row), $expectedColumnCount);

            yield ['message' => array_combine($this->header, $row)];
            $this->csvReader->next();
        }
    }
}
