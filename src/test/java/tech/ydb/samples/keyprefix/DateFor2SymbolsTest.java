package tech.ydb.samples.keyprefix;

import java.time.LocalDate;
import java.time.Month;
import java.time.temporal.ChronoUnit;
import org.junit.Test;

/**
 *
 * @author zinal
 */
public class DateFor2SymbolsTest {

    @Test
    public void miniTest() {
        LocalDate dt1 = LocalDate.parse("2024-02-19");
        LocalDate dt2 = LocalDate.parse("2024-02-20");
        String v1 = shortDate(dt1);
        String v2 = shortDate(dt2);
        System.out.println(v1 + " vs " + v2);
    }

    @Test
    public void test2SymbolsDate() {
        LocalDate date = LocalDate.of(2023, Month.JANUARY, 1);
        for (int i = 0; i < 5; ++i) {
            for (int j = 0; j < 25; ++j) {
                System.out.println(shortDate(date) + " " + date.toString());
                date = date.plus(1, ChronoUnit.DAYS);
            }
            date = date.plus(1, ChronoUnit.YEARS);
        }
    }

    public static String shortDate() {
        return shortDate(LocalDate.now());
    }

    public static String shortDate(LocalDate date) {
        // 0 - 3660
        int x = 366 * (date.getYear() % 11);
        // 0 - 4025
        x += date.getDayOfYear() - 1;
        char output[] = new char[2];
        output[1] = base64URLTable[x % 64];
        output[0] = base64URLTable[x / 64];
        return new String(output);
    }

    private static final char[] base64URLTable = {
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
        'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
        'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '-', '_'
    };
}
