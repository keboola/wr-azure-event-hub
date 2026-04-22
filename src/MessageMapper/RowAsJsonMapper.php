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

            if (count($row) !== $expectedColumnCount) {
                throw new UserException(sprintf(
                    'Row #%d has %d columns, but header has %d columns. '
                    . 'This usually happens when a field contains an unescaped newline '
                    . 'or CR character that breaks CSV parsing. '
                    . 'Strip "\n"/"\r" from multi-line text columns before writing.',
                    $rowNumber,
                    count($row),
                    $expectedColumnCount,
                ));
            }

            yield ['message' => array_combine($this->header, $row)];
            $this->csvReader->next();
        }
    }
}
