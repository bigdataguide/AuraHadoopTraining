package org.training.hadoop.hive;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import org.apache.hadoop.hive.ql.exec.UDF;

public class UDFZodiacSign extends UDF {

    private SimpleDateFormat df;

    public UDFZodiacSign() {
        df = new SimpleDateFormat("MM-dd-yyyy");
    }

    public String evaluate(Date bday) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(bday);
        int month = cal.get(Calendar.MONTH);
        int day = cal.get(Calendar.DAY_OF_MONTH);
        return evaluate(month + 1, day);
    }

    public String evaluate(String bday) {
        Date date = null;
        try {
            date = df.parse(bday);
        } catch (Exception ex) {
            System.out.println(ex);
            ex.printStackTrace();
            return null;
        }
        return evaluate(date);
    }

    public String evaluate(Integer month, Integer day) {
        if (month == 1) {
            if (day < 20) {
                return "Capricorn";
            } else {
                return "Aquarius";
            }
        }
        if (month == 2) {
            if (day < 19) {
                return "Capricorn";
            } else {
                return "Pisces";
            }
        }
        if (month == 3) {
            if (day < 20) {
                return "Pisces";
            } else {
                return "Aries";
            }
        }
        if (month == 4) {
            if (day < 20) {
                return "Aries";
            } else {
                return "Taurus";
            }
        }
        if (month == 5) {
            if (day < 20) {
                return "Taurus";
            } else {
                return "Gemini";
            }
        }
        if (month == 6) {
            if (day < 21) {
                return "Gemini";
            } else {
                return "Cancer";
            }
        }
        if (month == 7) {
            if (day < 22) {
                return "Cancer";
            } else {
                return "Leo";
            }
        }
        if (month == 8) {
            if (day < 23) {
                return "Leo";
            } else {
                return "Virgo";
            }
        }
        if (month == 9) {
            if (day < 22) {
                return "Virgo";
            } else {
                return "Libra";
            }
        }
        if (month == 10) {
            if (day < 24) {
                return "Libra";
            } else {
                return "Scorpio";
            }
        }
        if (month == 11) {
            if (day < 22) {
                return "Scorpio";
            } else {
                return "Sagittarius";
            }
        }
        if (month == 12) {
            if (day < 22) {
                return "Sagittarius";
            } else {
                return "Capricorn";
            }
        }

        return null;
    }

    public static void main(String[] args) {
        UDFZodiacSign aa = new UDFZodiacSign();
        String str = aa.evaluate("01-10-2004");
        System.out.println(str);
    }
}