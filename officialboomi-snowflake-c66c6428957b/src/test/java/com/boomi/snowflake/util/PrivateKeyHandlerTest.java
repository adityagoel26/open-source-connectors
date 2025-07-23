// Copyright (c) 2024 Boomi, Inc.
package com.boomi.snowflake.util;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCSException;
import org.bouncycastle.util.encoders.DecoderException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.InvalidKeyException;
import java.security.PrivateKey;

public class PrivateKeyHandlerTest {
    private static final String encryptedPrivateKey =
            "MIIFHDBOBgkqhkiG9w0BBQ0wQTApBgkqhkiG9w0BBQwwHAQIt+X205ylxyoCAggA "
                    + "MAwGCCqGSIb3DQIJBQAwFAYIKoZIhvcNAwcECD/Ox/97Cu63BIIEyIGHLJ5/evgE "
                    + "8p9b3eB62Wa7ZlwnSiGOv1/O8eA1uQy7t8ZVb0F+evBG2JzHvPIwc2o6XWDzh3ww "
                    + "NQlb1IypLB4fJmDMRbtbKGUzNaAxHGqnXcNk1O0q3N4LsmuI8l7DunFtUXTIPftq "
                    + "Ji7bahZ3c2/0BbRI+/R3W5W60EnX2mYyFWEvTRkThO5A86eV6QpdcO7yt5KVj+2K "
                    + "jDowRUUUR1HHuYdk/tOgCQVAaX3KBIBLa+BfoRpeLgFYyAOzeB3NrTxZGb0GPfls "
                    + "IbCc2tBD1/eYvLzMqy9dtIIypP4lmzzHGABNpQgRwNPDKfpRpxzCXuNXQ8XdXVJ5 "
                    + "r16hLiDVTxzIRaZyKu0IHpax7TBbJzR8GP9RyeO47gQK0Ok6cbCklx7/BeZOFHfJ "
                    + "S40d4UBbjpIwvqPqgn5+6wUmVwRIeCclTxlJzVPNX9aXyTFxHunlsuZ7zLcdYl/9 "
                    + "Ad9U0WqBgQ+uxUC1Vaaht0f4soJQOGv7W13KzHxxUL4ef3M2l3qbZlHuoEC5uZ// "
                    + "z7FDCHVaYt2S6k2ogoQx9dqOiM4A+fMWlDyf8N7tJW67y1exrZFJDRl3MkutBdru "
                    + "9d5le1Ng87FB6+CrTvmfMhb35/Ur1mcQzPFFAZQQRcdWnab9JajXcwPNQxlJGw2O "
                    + "6K9RhbGncOicpeEmmPHZbX9H/aUrVjACxgVulBvkiBcmTRqW6xQlPEYuR2aq0pHD "
                    + "6SVdoaQT2WAg7952Dcg2pFenGDakctz40C6/H1OgatwkvQ3BIetCGiFz+JSbkRIl "
                    + "K1IZQLXzSO93lfHrJz+ERfW5tL/vKbgcHGbvkLBxzKWag5uCWwMAtYTVSIYjCIjF "
                    + "7eb80cc40kyCdUy/HQBdQ59YbxKVebCO3DjyTquo11d1Tb+krlcVzQv5t5p4SP7f "
                    + "GqC9c+iaMsq9V7o0mZ+JnEPHDrJkBaWb99AjFnT89+fIfM+GBsEI0HUizHg7jki9 "
                    + "OHRYGKkceZRsqdzJgBujo2KeZmlNjcmiE3gtehHQ/t3o4Za5+4n7503m7JtIm9ZS "
                    + "VC5BOUxUaU4iU3roWoW5zlc1LMmPvuzrtcYNsPrjv4PsLfx+QDhCBq1tYGFoolvZ "
                    + "vTU1lPRoyHyLds6DZ2Pm2MaSbmS1cIS/iC8NHNfax1BjBds3K9gu9XsyJya/5QZH "
                    + "DmRFQHGpBeFkcscQGIkO/x3s3xb929RuHbl61Sf1NeJ2UvCmvgoHorRTXJ3AJKw3 "
                    + "V+azbIfIdFNmjo9pCAjsO+7e32iSgdmfy04xfzo1uFF3v9FXa0J3hqCajSS+PdPI "
                    + "o4tEu2WjO8UpnVrBcZyY6mb4CN2tL0N6NXOhqNE2TK7cW9mk5BoTvdU+eG0Yo7LZ "
                    + "DAj2BjSKXDNJfbFigNjcm8V1sy1rAaH2OtWVBEddimL2P9kBfb/nLg5vPCjrRSA5 "
                    + "lILnxfPjGfCU9CJg6dukhBs+UMUGE3ivyQ3Quih5+Q1/Dk7faWpB6iTJj0BGYsKj "
                    + "0lzaSd2knMGhY0J4A2Q9uclDBut+Fqt5UIz8WhtRmknx1HFeN6w2lYpc38bV2pDj "
                    + "68gEit5fTy+sjje7FY6DHMBE53NCrfytSt4k+bBpcrBya1UFEzJv9KwANniCtVTO GJpOF5E8sMFkbiNsdgdapA==";

    private static final String RSA_ENCRYPTED_KEY =
            "MIIFLTBXBgkqhkiG9w0BBQ0wSjApBgkqhkiG9w0BBQwwHAQI734daT+9T0ECAggA "
                    + "MAwGCCqGSIb3DQIJBQAwHQYJYIZIAWUDBAEqBBCKmE+rk8/bVbQ87ltDkuPFBIIE "
                    + "0FG/tkD2LnvHvS1qrfKTahoDtLBM5RC7h9sQ//CrhtT9Vkc8XxrY4tPNtyJHTZBO "
                    + "F8bLsfXSmAqK58jpS0tMp4ZKgrrROcpsZ4F6bStNU1KJ7SDHjHH3D+bPDqoYkYYA "
                    + "aBfk+aDOg0x6dMQtmaYNuUNwLvmRjq4rUXlSWqSKZTDLzHQr/lofurp08d+N387S "
                    + "ZCt1Xy/XHY5Kvjdi6SQKcKyE88WA2ty6SdhD7vZNdFvGxuIQ20mhaic1EP0v5c2S "
                    + "KiAwkR0cD6tOpxN804NVktBkvS+ASgUvdVPhm1tgkusSZmiH5erV9KTtzyYD9Xh5 "
                    + "v5BA/YFUiMKhPNDAtGZGF5qAzBuNcwWzwnFi9nt673qUOwYMiCeOew7VJGvEFkxq "
                    + "N3NboLvAYz87v3f0GAg4jGQFJDup/Vwowp5PZEfTz2U1aJFxKArrK87+UirJQGBg "
                    + "Xyxnec/n2qxVzTIiYPVGvha515FBPwDq7OoBniYdsTjp4TFlCnTcpjAcfuszlEqL "
                    + "RvhsodSoEyOY65Ic7VDDTx8xp5Fk4DPrw4yQYQCBwWRAwshziK/K5k6YmkIG8hkJ "
                    + "Hj8RaTgXwHTBXH/kWuMMMrlWa7FXcXVu+WqM+iv+vWFI2D/R83MuNVMf14ijS0bO "
                    + "qa9YwrKdgRRUulQYtipWAT5pLa1XUSNFZ1RyYdEYPNpBCyYhlj5zQ1IgAyo9OcaS "
                    + "PS9knIcAXuT6PaQA3mBjtMpux6FMnWl0NdrNYZu4j7zT2JKhH8zKKkJy61ZK5g1p "
                    + "LKfQg8Cnbdx9n07kaJbkm8bhZL0e1lXFmCAUFtSHPvfv/GAc1I32xvnR5l6J8w3e "
                    + "zRMFKSdOeJjA9iuisF4gMJMsHetGxBp5HDSkeJeE3CC/zS9Nk762ddVp9QImPLNT "
                    + "fr++OXFW4t6K2DUPcQ8FpdsYQQOrqUwjKAmtRKzM35pm8Uv5BzfrqCnU8l/rUtn2 "
                    + "fBtYgK2hebh/VmelFbpNq1zCVfztzryQU85zRaoghmSNrCklKGx3TucP52lR+0Hz "
                    + "J6ipPnojEqiz61ah8amd/fFAgbh0sp7LWjiuLGOiMSWq+5E+wxIOnU040IG18TxU "
                    + "6Nepn2igk26yUxuEvuUcbo2U1UQKJiG7dJpoem5+ZIjMbk+rJMNWRlNE8dGW+aSV "
                    + "QYcn3RSip7CDnsB2vEEvyl9Op82YBfJxR1IfJQFHQQbhWDou6+WeS2yV713U2DtW "
                    + "QwRPhr9DP2izIdXMLFsjykFE20fvQDRIVyNLzG43EUccw39N2B0fWUd9CfZJELMS "
                    + "JrqWTXlux9is80fi3fVVYh8gTxVipRc8dpHyhMoIGxdqAOEwUwI0rht4HOCZEmsP "
                    + "Ses3ji1CVEoHyv/f7306HFJNs1ySMYAOEJHmfePYdIkZB4GMEjr10zHO7OHntXOP "
                    + "QSSTufc0SmmXZv13c/pnOh9k7XsD/OTd8eZwfQvH2LW5M2IqQjVI2XLMFLIz4HKN "
                    + "oDiM+wJ2GehRS4Mt6wHP0su7i0peF35WTp70Eef7/XXGOhjcFfc30OqDGtj2DElQ "
                    + "6QYiQxk1aPv+bfTrxZdw7UFcK3gwwRK1b5yfDM3nL+2IYPC58WSwVpGJSvffU9AK "
                    + "mkDegJARYiz8trvtMb3eZ5EPqgBEd3hxctRHGoL2eQtI";
    private static final String OPENSSL_0_9_8_PRIVATE_KEY =
            "MIIE6TAbBgkqhkiG9w0BBQMwDgQIzFuqKWfMvuYCAggABIIEyAJKbNtCD3UIQItn "
                    + "kf/ZPaZY7ZALFqs9H9tqXhxb2ZsN27jnmAHZsl6TPzPEp/fLaKkvWLWdBgyfKN5m "
                    + "HvtDgK+EYmJXWuZujqHF4ZvFHmVTXMsVJLAn18yJaOGdqsGMV6AyTFeadhRiJRhY "
                    + "4Qzz80f9uOJiFI8i7J7hWKYE5MJmAhuLgUvxm+EAOW7t5WVnSWryj2o9bRdhmgRz "
                    + "u3vqhL/wTg7cLptxZgxdajB9NniTN8PHCU2b7euCZpHjxVY1pczlfFFfZqDaKKzj "
                    + "Nop9P7CFqp8IWPcSg1+zCuXwZ3TIIMl4uXYKs5SFLE6PE1qbYvCm0DIT1lzHF7h5 "
                    + "lYzi+UedK3Yle1O0xvWhkB5/kQZWBnYCqKDKCJXDV1UIY5zp/P2c4zzCHpaGXOtA "
                    + "aYcYTsKGW5+KuE5iR+LyCfcxF4Z9ECGIO6I9x8dMIqJWbp6GWOQvl0kwqpdr/XXU "
                    + "5SHCmmvMEiKlRwFnblTxFvhFFNYni8qGiC4UQSw5t0wFAxmxrT9wh5CqeyClxRmk "
                    + "6hretNoyH9peJdytkl+R4aIqWxXWnmlIo+wYD40D29GMO5k6YFY7JwEvx4i87D09 "
                    + "YjuAm7MUlFOoH4+OaJxHxZZ63ZmaMts/mKWElAq8Ayw1WqamtJAgC/DHuap4k0Lk "
                    + "NfuEnCLE5U21/dbHQwoXQVQF7gVy9FI7RRbRjw1WdQr5iSoxua8JfB1NRDUQYiDi "
                    + "zHWZSO4GwM3NPg3D1SM8IqzW5fOUTzCZGTFaW2k4CiCtHYWhKks7L25fzykfhLbs "
                    + "59Y1q89YtyHlBBVU7nH9sGhkEmUt7uFiUv7iwfrBHvxApgxo6AFBR9KP9swYk7Xw "
                    + "LSs5pm1FTusyMFI2bBnyq/eRGD8tsaZ8F7dKPMeIxJ+t8/8dOuYL+vp5Emk8vcQv "
                    + "PaDE5C9UIj7wH4KCIrJUeDABk0bHRBx57dmWRG7a+opoc1S2PO+tZWzJH2PfewKH "
                    + "rQ9RVwqGS2ERmrtTpB9rT2I9vE+ydEURhQMsa9iTBjhVPdpRLn+bX06WJ+fuy5+d "
                    + "MUwbiMVbFjH1tn8GdQPf74bHDyhejld1rBEnRvBJ9212Isc6QsWsiyvHkSze7HuM "
                    + "EzR3s5jx2DN8/eMM016Z1UpPY7M/J4rTYV5Vnj96aMGqZqvq4LH1dCOKVYcQixoo "
                    + "GWK2+gMXOTzWrrxf/JtUDl4Kb48Tbw7QrfqFeFStXM8veFrpUw7jSOOofOACtEDh "
                    + "o9xgKSoJCwZv9FIC1RXLW6S7xvwxcFaYqMhiq0ggU7wAWOvg9NWA/4xbcbT//It0 "
                    + "hDeEpAFqdzLOZytbxPkNyYiCbLtkZVU6B8fzZKHZI6XBM3HpDmVyYDkxzLSFiMC9 "
                    + "aHQIMPt+Idi0MYquWVrj7Kk+dT6dvKIXW0OM1ihoMYtD3xrPS16WrDJYRXs5qESk "
                    + "aDHBqXETWwd9o/O9xWQV0NLItErVjCREN63QZ4CEmBsTcdezyzIieWKZYhgfWNFW "
                    + "Z0apzb0tt8Q96zS2CVhA8q7pKHYS6hdKwRh+4brfX0jko6HdKr1B19wV3dN810rB "
                    + "sfF6K28mrvrMQs3WEjfNQG1uSbyhdKhXxi882vymjpDl1+MSe5WLfSz+6QQR7wJy vJu9Mbdby24qyU8Jhw==";

    private static final String OPENSSL_1_1_1_PRIVATE_KEY =
            "MIIFLTBXBgkqhkiG9w0BBQ0wSjApBgkqhkiG9w0BBQwwHAQIoqyeVpFLD70CAggA "
                    + "MAwGCCqGSIb3DQIJBQAwHQYJYIZIAWUDBAEqBBDYXu7TD/KugFDlKYk/xv6mBIIE "
                    + "0O+/VGQN0+VqEu/ykHo+sIlpj/rralTsHVuIwPSS1BNTh0D2Symgt0ap+N2QN0AS "
                    + "rUlE3LOt8mgVdYbWnRcGrE0d2GV4zXvciHUrAktdxQKTW10nZUB4hE9NwbjIpNWp "
                    + "lFrw/TgYwExDj+EphQvOaXThLv7EvwaqSFPI169XHHqfBIbXqKJON6yR8ia9CXKr "
                    + "82zh73YRF42l4AvXuQNZMBlnTrc8wwt9aVSqQtQmVt3zHISd8WcKBRLQk3rNFoQC "
                    + "9cqQZ0c5k+WMbWMV1GdpwuRGcsTg/5Wf8tDm/xDJgyWddgNaaPPskDbRoU2LXirO "
                    + "6wDN/BdcAwR1ysRTwvti3p8tZwaYKOP/SqV5Ein5TkGEIVOYU4jbVUhPcyGT13A/ "
                    + "EZzsL8nV9EmsWKbGjvxc06QD6fL6vnzqrtREZCawXCro7E+F1NEB31MGO1K1Cmr0 "
                    + "gxWoSddpyrAFei9hTtl5yJTsKVG3uh/pt+ouFV6nXHLkSTstUS2HUQY8kP89Gr3K "
                    + "R2ZIFWu81SDYh0aR8nN1Y1uJj2Vi/Eduh3dSvtAcjj+irIoO22iz4GOnX94e9E7I "
                    + "dVY9xtcP7bNq9LMUPV/CDuT/AC4qPVOwCGbVxsrCI+q6lyQXxCVIda12e1bJYJsz "
                    + "nJLvc2d0NlndCr3L4pgz13wC+W6QQpgRvWAee+xjI29GRMe5wPo/USQ0gKZ5VABL "
                    + "qJAvAUriNxupizgkUkeL0mdvtdhckMkX+MBLqUCQZpb2uJuFKP6qjWBBDOqiMg6U "
                    + "r/U52BbDbHxbTwLR+RVVyRuzjRHbMUhO4zYK4/PCzFwNtYT/2ZkRg+u0MZZQq6lY "
                    + "+w+3bN6WNBrjLctZ7iA4AHgIAuypOhj1lYf8ZlnPg4i6GGo5cvTp3x6Y8UoT3xdF "
                    + "7Y8qayaREdV5aaaFw+4mmFhpfOTcd4i6IVWnExQfXoWi5rBmEyymhR6ZrccS4t3g "
                    + "ujjIyG/1SC7yMwyNGsv5QSUapTxGqPKMKppeEkgAzEolfowobu4vY5MTdcEiuvyO "
                    + "Hp2SY9oC19pglwZpoc23fAm9xrvb/KL8S4s60iZZm1e1Y5NS+CN3d6LUoD5avQyb "
                    + "guvN7XYPjyLHC2lMbvs/37d2Ma9ACz72w64BGtCtRSVrCXWrxxq1hkkoURXjKWGz "
                    + "MDsHTRVCy08jm6mIH/SDlVgdrw2AhDpwozqeJyoZCGhqjgSyTVjHz/dx18Kzh8QB "
                    + "FmHaPfHP9ZtDbsrhkCHgxfpP4PKPQRZK3wamSa3T8RakOqCBEWnNBpROXwn7yIvx "
                    + "zt86hY6SkbYIyilD5k6UO+GpzHQS+KDtaqGmtclYeFZZpZyBy4W28K7sdzjUFF6k "
                    + "n+Luq7P2cdVTmddOwaFl5ppHvZjesXBR48IXxFPfJnqWCtwd+TYSeO4QzivjQN+n "
                    + "RNL40WTVHZPtVMQgQs9BukQ1cpjrPTD1uNM/ky8hS0NvjcfC4E+jIbAf65i5nFgN "
                    + "HiMNBP7BzKmMDUV7PxWoE0qUkVUGPYMngUXU5twzPTXow/O9ryun3MNYY2AFY78v "
                    + "A5fcwD+PtyC93LL2nRAEXJOcVenWjPeUo/kJO9brlW6IaijPW0oet8HHkCr02hit "
                    + "RwUOK1MnTZOyqOTOvzi7xIBaAv2SGdYEyT8zzPHIz45o";
        private static final String passphrase = "private_key_passphrase";
    private static final String passphrase_wrong = "invalid_private_key_passphrase";
    private static final String algorithm = "RSA";
    private static final String privateKeyString = "TestInvalidPrivateKey";

    /**
     * This method will verify the OPENSSL 3.x generated private key
     */
    @Test
    public void getPrivateObjectEncrypted()
            throws IOException, InvalidKeyException, OperatorCreationException, PKCSException {

        PrivateKey privateKey = PrivateKeyHandler.getPrivateObject(encryptedPrivateKey, passphrase.toCharArray());
        Assert.assertNotNull(privateKey);
        Assert.assertEquals(algorithm, privateKey.getAlgorithm());
    }

    @Test(expected = PKCSException.class)
    public void getPrivateObjectEncryptedInvalidKeyException()
            throws InvalidKeyException, IOException, OperatorCreationException, PKCSException {
        PrivateKeyHandler.getPrivateObject(encryptedPrivateKey, passphrase_wrong.toCharArray());
    }

    @Test(expected = DecoderException.class)
    public void getPrivateObjectEncryptedInvalidKeyException1()
            throws InvalidKeyException, IOException, OperatorCreationException, PKCSException {
        PrivateKeyHandler.getPrivateObject(privateKeyString, passphrase_wrong.toCharArray());
    }

    /**
     * This method will verify the RSA python generated private key
     */
    @Test
    public void getPrivateRSAEncryptedInvalidKeyException()
            throws InvalidKeyException, IOException, OperatorCreationException, PKCSException {
        PrivateKey privateKey = PrivateKeyHandler.getPrivateObject(RSA_ENCRYPTED_KEY, passphrase.toCharArray());
        Assert.assertNotNull(privateKey);
        Assert.assertEquals(algorithm, privateKey.getAlgorithm());
    }
    @Test(expected = PKCSException.class)
    public void getPrivateRSAEncryptedInvalidKeyException1()
            throws InvalidKeyException, IOException, OperatorCreationException, PKCSException {
        PrivateKeyHandler.getPrivateObject(RSA_ENCRYPTED_KEY, passphrase_wrong.toCharArray());
    }

    /**
     * This method will verify the OPENSSL 0.9.8 private key
     */
    @Test
    public void getPrivateOpenSSL_Encrypted0_9_8h()
            throws InvalidKeyException, IOException, OperatorCreationException, PKCSException {
        PrivateKey privateKey = PrivateKeyHandler.getPrivateObject(OPENSSL_0_9_8_PRIVATE_KEY, passphrase.toCharArray());
        Assert.assertNotNull(privateKey);
        Assert.assertEquals(algorithm, privateKey.getAlgorithm());
    }

    /**
     * This method will verify the OPENSSL 1.1.1 private key
     */
    @Test
    public void getPrivateOpenSSL_Encrypted1_1_1_()
            throws InvalidKeyException, IOException, OperatorCreationException, PKCSException {
        PrivateKey privateKey = PrivateKeyHandler.getPrivateObject(OPENSSL_1_1_1_PRIVATE_KEY, passphrase.toCharArray());
        Assert.assertNotNull(privateKey);
        Assert.assertEquals(algorithm, privateKey.getAlgorithm());
    }

    /**
     * This method is verify the Multiple calls should return the same provider instance.
     */
    @Test
    public void getProviderInstance() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method method = PrivateKeyHandler.class.getDeclaredMethod("getProviderInstance");
        method.setAccessible(true);
        BouncyCastleProvider bouncyCastleProvider1 = (BouncyCastleProvider) method.invoke(null);
        Assert.assertNotNull(bouncyCastleProvider1);
        BouncyCastleProvider bouncyCastleProvider2 = (BouncyCastleProvider) method.invoke(null);
        Assert.assertNotNull(bouncyCastleProvider2);
        Assert.assertEquals("Multiple calls should return the same provider instance", bouncyCastleProvider1, bouncyCastleProvider2);
    }
}
