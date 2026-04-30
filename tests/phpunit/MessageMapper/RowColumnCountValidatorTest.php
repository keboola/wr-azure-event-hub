<?php

declare(strict_types=1);

namespace Keboola\AzureEventHubWriter\Tests\MessageMapper;

use Generator;
use Keboola\AzureEventHubWriter\Exception\UserException;
use Keboola\AzureEventHubWriter\MessageMapper\RowColumnCountValidator;
use PHPUnit\Framework\TestCase;

class RowColumnCountValidatorTest extends TestCase
{
    /**
     * @dataProvider provideMatchingCounts
     */
    public function testNoExceptionWhenCountsMatch(int $rowNumber, int $columnCount): void
    {
        RowColumnCountValidator::assertMatchesHeader($rowNumber, $columnCount, $columnCount);

        // No exception thrown.
        self::assertTrue(true);
    }

    public static function provideMatchingCounts(): Generator
    {
        yield 'first row, single column' => [1, 1];
        yield 'first row, three columns' => [1, 3];
        yield 'later row, three columns' => [42, 3];
        yield 'wide row' => [7, 100];
    }

    /**
     * @dataProvider provideMismatches
     */
    public function testThrowsOnMismatch(
        int $rowNumber,
        int $rowColumnCount,
        int $headerColumnCount,
        string $expectedRowLabel,
    ): void {
        $this->expectException(UserException::class);
        $this->expectExceptionMessage(sprintf(
            '%s has %d columns, but header has %d columns.',
            $expectedRowLabel,
            $rowColumnCount,
            $headerColumnCount,
        ));

        RowColumnCountValidator::assertMatchesHeader($rowNumber, $rowColumnCount, $headerColumnCount);
    }

    public static function provideMismatches(): Generator
    {
        yield 'first row, fewer columns' => [
            'rowNumber' => 1,
            'rowColumnCount' => 2,
            'headerColumnCount' => 3,
            'expectedRowLabel' => 'Row #1',
        ];
        yield 'later row, fewer columns' => [
            'rowNumber' => 5,
            'rowColumnCount' => 1,
            'headerColumnCount' => 3,
            'expectedRowLabel' => 'Row #5',
        ];
        yield 'first row, more columns' => [
            'rowNumber' => 1,
            'rowColumnCount' => 4,
            'headerColumnCount' => 3,
            'expectedRowLabel' => 'Row #1',
        ];
        yield 'later row, more columns' => [
            'rowNumber' => 12,
            'rowColumnCount' => 5,
            'headerColumnCount' => 3,
            'expectedRowLabel' => 'Row #12',
        ];
    }

    public function testErrorMessageMentionsBothCommonCauses(): void
    {
        try {
            RowColumnCountValidator::assertMatchesHeader(2, 2, 3);
            self::fail('Expected UserException was not thrown.');
        } catch (UserException $e) {
            $message = $e->getMessage();
            self::assertStringContainsString('newline/CR', $message);
            self::assertStringContainsString('unquoted delimiter', $message);
            self::assertStringContainsString('properly quoted', $message);
        }
    }
}
