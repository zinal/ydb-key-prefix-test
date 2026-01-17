package tech.ydb.samples.keyprefix;

import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import org.junit.Test;

/**
 *
 * @author zinal
 */
public class TimerSpeedTest {

    @Test
    public void testTimerSpeed() {
        var before = Instant.now();
        int megamask = 0;
        for (int i = 0; i < 1000000; ++i) {
            megamask ^= timerAction();
            megamask = megamask >>> 1;
        }
        var duration = before.until(Instant.now(), ChronoUnit.MILLIS);
        System.out.println("** Duration of " + duration + " milliseconds (mask " + megamask + ")");
    }

    private int timerAction() {
        LocalDate date = LocalDate.now();
        return date.getDayOfYear() + (366 * (date.getYear() % 40));
    }

}
