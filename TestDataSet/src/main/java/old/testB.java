package old;

import java.util.ArrayList;

public class testB {
    
    public static void main(String[] args){ 
        testB testB = new testB();
        boolean[] flags = testB.calcRegFlags("trialuser","064-4439-7940-102362");
        System.out.println(flags);
        for (boolean flag : flags) {
            System.out.println(flag);
        }
    }

    public boolean[] calcRegFlags(String name, String key) {
        if (name.length() < 5) {
            return null;
        } else {
            StringBuffer var3 = new StringBuffer();

            for(int var4 = 0; var4 < key.length(); ++var4) {
                char var5 = key.charAt(var4);
                if (var5 >= '0' && var5 <= '9') {
                    var3.append(var5);
                }
            }

            if (var3.length() == 15) {
                return this.b(name, var3.toString(), 6);
            } else if (var3.length() == 17) {
                return this.b(name, var3.toString(), 8);
            } else {
                return null;
            }
        }
    }

    private boolean[] b(String var1, String var2, int var3) {
        int var4 = 0;

        int var5;
        for(var5 = 0; var5 < 5 + var3; ++var5) {
            var4 += var2.charAt(var5) - 48;
        }

        ArrayList var6;
        int var8;
        for(var6 = new ArrayList(); var5 < var2.length(); ++var5) {
            char var7 = (char)(var2.charAt(var5) - 48);

            for(var8 = var7; var8 - var4 < 0; var8 += 10) {
            }

            int var9 = (var8 - var4) % 10;
            var4 += var7;

            for(int var10 = 0; var10 < 3; ++var10) {
                var6.add(new Boolean((var9 & 1 << var10) != 0));
            }
        }

        boolean[] var11 = new boolean[var6.size()];

        for(var8 = 0; var8 < var6.size(); ++var8) {
            var11[var8] = (Boolean)var6.get(var8);
        }

        return var11;
    }
}
