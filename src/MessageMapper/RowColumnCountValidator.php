<?php

declare(strict_types=1);

namespace Keboola\AzureEventHubWriter\MessageMapper;

use Keboola\AzureEventHubWriter\Exception\UserException;

class RowColumnCountValidator
{
    /**
     * Validates that a CSV row has the same number of columns as the header.
     *
     * Throws a UserException with the affected row number and expected/actual
     * column counts if they do not match. The wording covers both common
     * causes of a mismatch:
     * - too few columns: typically an unescaped newline/CR character inside
     *   a field that breaks CSV parsing,
     * - too many columns: typically an unquoted delimiter (e.g. an extra comma)
     *   inside a field.
     *
     * @param int $rowNumber 1-based row number (header excluded), used in the
     *                       error message to point the user to the bad row.
     */
    public static function assertMatchesHeader(
        int $rowNumber,
        int $rowColumnCount,
        int $headerColumnCount,
    ): void {
        if ($rowColumnCount === $headerColumnCount) {
            return;
        }

        throw new UserException(sprintf(
            'Row #%d has %d columns, but header has %d columns. '
            . 'This usually happens when a field contains an unescaped newline/CR '
            . 'or an unquoted delimiter that breaks CSV parsing. '
            . 'Ensure all fields with special characters are properly quoted in the input table.',
            $rowNumber,
            $rowColumnCount,
            $headerColumnCount,
        ));
    }
}
