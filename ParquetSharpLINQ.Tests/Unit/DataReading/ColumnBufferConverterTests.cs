using System.Collections;
using System.Reflection;

namespace ParquetSharpLINQ.Tests.Unit.DataReading
{
    [TestFixture]
    [Category("Unit")]
    [Category("DataReading")]
    public class ColumnBufferConverterTests
    {
        // Helper to invoke the generic Convert<TSource,TTarget> method via reflection.
        private static object? InvokeConvert(object? source, Type sourceType, Type targetType)
        {
            var converterType = typeof(ParquetSharp.Buffers.Converter.ColumnBufferConverter);
            var method = converterType.GetMethod("Convert", BindingFlags.Public | BindingFlags.Static);
            if (method is null) throw new InvalidOperationException("Convert method not found");

            var generic = method.MakeGenericMethod(sourceType, targetType);

            try
            {
                return generic.Invoke(null, [source]);
            }
            catch (TargetInvocationException tie) when (tie.InnerException != null)
            {
                // Rethrow the inner exception so NUnit sees the original exception type
                throw tie.InnerException;
            }
        }

        private static void AssertEqualWithTolerance(object? expected, object? actual, Type targetType)
        {
            if (expected is null || actual is null)
            {
                Assert.That(actual, Is.EqualTo(expected));
                return;
            }

            if (targetType == typeof(float) || targetType == typeof(float?))
            {
                Assert.That(Convert.ToDouble(actual), Is.EqualTo(Convert.ToDouble(expected)).Within(1e-6));
                return;
            }

            if (targetType == typeof(double) || targetType == typeof(double?))
            {
                Assert.That(Convert.ToDouble(actual), Is.EqualTo(Convert.ToDouble(expected)).Within(1e-9));
                return;
            }

            Assert.That(actual, Is.EqualTo(expected));
        }

        public static IEnumerable SuccessfulConversions
        {
            get
            {
                yield return new TestCaseData((byte)5, typeof(byte), typeof(byte), (byte)5).SetName("byte->byte");
                yield return new TestCaseData((byte)5, typeof(byte), typeof(short), (short)5).SetName("byte->short");
                yield return new TestCaseData((byte)5, typeof(byte), typeof(ushort), (ushort)5).SetName("byte->ushort");
                yield return new TestCaseData((byte)5, typeof(byte), typeof(int), 5).SetName("byte->int");
                yield return new TestCaseData((byte)5, typeof(byte), typeof(uint), (uint)5).SetName("byte->uint");
                yield return new TestCaseData((byte)5, typeof(byte), typeof(long), (long)5).SetName("byte->long");
                yield return new TestCaseData((byte)5, typeof(byte), typeof(ulong), (ulong)5).SetName("byte->ulong");
                yield return new TestCaseData((byte)5, typeof(byte), typeof(float), (float)5).SetName("byte->float");
                yield return new TestCaseData((byte)5, typeof(byte), typeof(double), (double)5).SetName("byte->double");
                yield return new TestCaseData((byte)5, typeof(byte), typeof(decimal), (decimal)5).SetName("byte->decimal");
                yield return new TestCaseData(byte.MaxValue, typeof(byte), typeof(ulong), (ulong)byte.MaxValue).SetName("byteMax->ulong");

                yield return new TestCaseData((sbyte)-3, typeof(sbyte), typeof(sbyte), (sbyte)-3).SetName("sbyte->sbyte");
                yield return new TestCaseData((sbyte)-3, typeof(sbyte), typeof(short), (short)-3).SetName("sbyte->short");
                yield return new TestCaseData((sbyte)-3, typeof(sbyte), typeof(int), -3).SetName("sbyte->int");
                yield return new TestCaseData((sbyte)-3, typeof(sbyte), typeof(long), (long)-3).SetName("sbyte->long");
                yield return new TestCaseData((sbyte)-3, typeof(sbyte), typeof(float), (float)-3).SetName("sbyte->float");
                yield return new TestCaseData((sbyte)-3, typeof(sbyte), typeof(double), (double)-3).SetName("sbyte->double");
                yield return new TestCaseData((sbyte)-3, typeof(sbyte), typeof(decimal), (decimal)-3).SetName("sbyte->decimal");

                yield return new TestCaseData((short)123, typeof(short), typeof(short), (short)123).SetName("short->short");
                yield return new TestCaseData((short)123, typeof(short), typeof(int), 123).SetName("short->int");
                yield return new TestCaseData((short)123, typeof(short), typeof(long), (long)123).SetName("short->long");
                yield return new TestCaseData((short)123, typeof(short), typeof(float), (float)123).SetName("short->float");
                yield return new TestCaseData((short)123, typeof(short), typeof(double), (double)123).SetName("short->double");
                yield return new TestCaseData((short)123, typeof(short), typeof(decimal), (decimal)123).SetName("short->decimal");

                yield return new TestCaseData((ushort)123, typeof(ushort), typeof(ushort), (ushort)123).SetName("ushort->ushort");
                yield return new TestCaseData((ushort)123, typeof(ushort), typeof(uint), (uint)123).SetName("ushort->uint");
                yield return new TestCaseData((ushort)123, typeof(ushort), typeof(ulong), (ulong)123).SetName("ushort->ulong");
                yield return new TestCaseData((ushort)123, typeof(ushort), typeof(float), (float)123).SetName("ushort->float");
                yield return new TestCaseData((ushort)123, typeof(ushort), typeof(double), (double)123).SetName("ushort->double");
                yield return new TestCaseData((ushort)123, typeof(ushort), typeof(decimal), (decimal)123).SetName("ushort->decimal");
                yield return new TestCaseData(ushort.MaxValue, typeof(ushort), typeof(uint), (uint)ushort.MaxValue).SetName("ushortMax->uint");

                yield return new TestCaseData(42, typeof(int), typeof(int), 42).SetName("int->int");
                yield return new TestCaseData(42, typeof(int), typeof(long), (long)42).SetName("int->long");
                yield return new TestCaseData(42, typeof(int), typeof(float), (float)42).SetName("int->float");
                yield return new TestCaseData(42, typeof(int), typeof(double), (double)42).SetName("int->double");
                yield return new TestCaseData(42, typeof(int), typeof(decimal), (decimal)42).SetName("int->decimal");
                yield return new TestCaseData(int.MinValue, typeof(int), typeof(long), (long)int.MinValue).SetName("intMin->long");
                yield return new TestCaseData(int.MaxValue, typeof(int), typeof(decimal), (decimal)int.MaxValue).SetName("intMax->decimal");

                yield return new TestCaseData((uint)42, typeof(uint), typeof(uint), (uint)42).SetName("uint->uint");
                yield return new TestCaseData((uint)42, typeof(uint), typeof(ulong), (ulong)42).SetName("uint->ulong");
                yield return new TestCaseData((uint)42, typeof(uint), typeof(float), (float)42).SetName("uint->float");
                yield return new TestCaseData((uint)42, typeof(uint), typeof(double), (double)42).SetName("uint->double");
                yield return new TestCaseData((uint)42, typeof(uint), typeof(decimal), (decimal)42).SetName("uint->decimal");
                yield return new TestCaseData(uint.MaxValue, typeof(uint), typeof(ulong), (ulong)uint.MaxValue).SetName("uintMax->ulong");

                yield return new TestCaseData((long)1234567890123, typeof(long), typeof(long), (long)1234567890123).SetName("long->long");
                yield return new TestCaseData((long)5, typeof(long), typeof(float), (float)5).SetName("long->float");
                yield return new TestCaseData((long)5, typeof(long), typeof(double), (double)5).SetName("long->double");
                yield return new TestCaseData((long)5, typeof(long), typeof(decimal), (decimal)5).SetName("long->decimal");
                yield return new TestCaseData(long.MaxValue, typeof(long), typeof(decimal), (decimal)long.MaxValue).SetName("longMax->decimal");

                yield return new TestCaseData((ulong)1234567890123, typeof(ulong), typeof(ulong), (ulong)1234567890123).SetName("ulong->ulong");
                yield return new TestCaseData((ulong)5, typeof(ulong), typeof(float), (float)5).SetName("ulong->float");
                yield return new TestCaseData((ulong)5, typeof(ulong), typeof(double), (double)5).SetName("ulong->double");
                yield return new TestCaseData((ulong)5, typeof(ulong), typeof(decimal), (decimal)5).SetName("ulong->decimal");
                yield return new TestCaseData(ulong.MaxValue, typeof(ulong), typeof(decimal), (decimal)ulong.MaxValue).SetName("ulongMax->decimal");

                yield return new TestCaseData((decimal)12.34m, typeof(decimal), typeof(decimal), (decimal)12.34m).SetName("decimal->decimal");
                yield return new TestCaseData((decimal)-12.34m, typeof(decimal), typeof(decimal), (decimal)-12.34m).SetName("decimalNegative->decimal");
                yield return new TestCaseData((decimal)12.34m, typeof(decimal), typeof(double), (double)12.34m).SetName("decimal->double");
                yield return new TestCaseData(decimal.MaxValue, typeof(decimal), typeof(double), (double)decimal.MaxValue).SetName("decimalMax->double");

                yield return new TestCaseData((double)3.1415, typeof(double), typeof(double), (double)3.1415).SetName("double->double");
                yield return new TestCaseData((double)3, typeof(double), typeof(decimal), (decimal)3).SetName("double->decimal");

                yield return new TestCaseData((float)2.5f, typeof(float), typeof(float), (float)2.5f).SetName("float->float");
                yield return new TestCaseData((float)2.5f, typeof(float), typeof(double), (double)2.5f).SetName("float->double");
                yield return new TestCaseData((float)2.0f, typeof(float), typeof(decimal), (decimal)2).SetName("float->decimal");
                yield return new TestCaseData(float.MaxValue, typeof(float), typeof(double), (double)float.MaxValue).SetName("floatMax->double");
                yield return new TestCaseData(float.PositiveInfinity, typeof(float), typeof(double), double.PositiveInfinity).SetName("floatInf->doubleInf");
                yield return new TestCaseData(float.NaN, typeof(float), typeof(double), double.NaN).SetName("floatNaN->doubleNaN");
                yield return new TestCaseData(sbyte.MaxValue, typeof(sbyte), typeof(int), (int)sbyte.MaxValue).SetName("sbyteMax->int");
                yield return new TestCaseData(short.MaxValue, typeof(short), typeof(int), (int)short.MaxValue).SetName("shortMax->int");
                yield return new TestCaseData(short.MinValue, typeof(short), typeof(long), (long)short.MinValue).SetName("shortMin->long");

                yield return new TestCaseData(true, typeof(bool), typeof(bool), true).SetName("bool->bool");
                yield return new TestCaseData(true, typeof(bool), typeof(bool?), true).SetName("bool->boolNullable");
                yield return new TestCaseData(true, typeof(bool?), typeof(bool), true).SetName("boolNullable->bool");
                yield return new TestCaseData(null, typeof(bool?), typeof(bool?), null).SetName("boolNullableNull->boolNullable");

                yield return new TestCaseData("123", typeof(string), typeof(byte), (byte)123).SetName("string->byte");
                yield return new TestCaseData("+123", typeof(string), typeof(int), 123).SetName("stringPlus->int");
                yield return new TestCaseData("  42  ", typeof(string), typeof(int), 42).SetName("stringWhitespace->int");
                yield return new TestCaseData("0001", typeof(string), typeof(int), 1).SetName("stringLeadingZeros->int");
                yield return new TestCaseData("+1.5", typeof(string), typeof(double), (double)1.5).SetName("stringPlusFloat->double");
                yield return new TestCaseData("  +1.5  ", typeof(string), typeof(float), (float)1.5).SetName("stringWhitespacePlusFloat->float");
                yield return new TestCaseData("-12", typeof(string), typeof(short), (short)-12).SetName("string->short");
                yield return new TestCaseData("1234", typeof(string), typeof(int), 1234).SetName("string->int");
                yield return new TestCaseData("1234567890123", typeof(string), typeof(long), 1234567890123L).SetName("string->long");
                yield return new TestCaseData("1.5", typeof(string), typeof(float), (float)1.5).SetName("string->float");
                yield return new TestCaseData("2.25", typeof(string), typeof(double), (double)2.25).SetName("string->double");
                yield return new TestCaseData("3.5", typeof(string), typeof(decimal), (decimal)3.5).SetName("string->decimal");
                yield return new TestCaseData("NaN", typeof(string), typeof(double), double.NaN).SetName("stringNaN->doubleNaN");
                yield return new TestCaseData("Infinity", typeof(string), typeof(double), double.PositiveInfinity).SetName("stringInf->doubleInf");
                yield return new TestCaseData("false", typeof(string), typeof(bool), false).SetName("stringFalse->bool");
                yield return new TestCaseData("true", typeof(string), typeof(bool), true).SetName("string->bool");
                yield return new TestCaseData("2000-01-02", typeof(string), typeof(DateOnly), new DateOnly(2000, 1, 2)).SetName("string->DateOnly");
                yield return new TestCaseData("15:30:00", typeof(string), typeof(TimeOnly), new TimeOnly(15, 30, 0)).SetName("string->TimeOnly");
                yield return new TestCaseData("2020-12-31T23:59:59Z", typeof(string), typeof(DateTime), new DateTime(2020, 12, 31, 23, 59, 59, DateTimeKind.Utc)).SetName("string->DateTimeUTC");
                yield return new TestCaseData("2020-12-31T23:59:59+01:00", typeof(string), typeof(DateTime), new DateTime(2020, 12, 31, 22, 59, 59, DateTimeKind.Utc)).SetName("stringOffset->DateTimeUTC");
                yield return new TestCaseData("2020-12-31T23:59:59", typeof(string), typeof(DateTime), new DateTime(2020, 12, 31, 23, 59, 59, DateTimeKind.Utc)).SetName("stringNoZone->DateTimeUTC");
                yield return new TestCaseData("15:30:00.123", typeof(string), typeof(TimeOnly), new TimeOnly(15, 30, 0, 123)).SetName("stringTimeMs->TimeOnly");
                yield return new TestCaseData("01:02:03", typeof(string), typeof(TimeSpan), new TimeSpan(1, 2, 3)).SetName("string->TimeSpan");
                var guid = Guid.NewGuid();
                yield return new TestCaseData(guid.ToString(), typeof(string), typeof(Guid), guid).SetName("string->Guid");
                yield return new TestCaseData("hello", typeof(string), typeof(string), "hello").SetName("string->string");

                var someDate = new DateTime(2021, 6, 1, 0, 0, 0, DateTimeKind.Utc);
                var parquetDate = new global::ParquetSharp.Date(someDate);
                yield return new TestCaseData(parquetDate, typeof(global::ParquetSharp.Date), typeof(global::ParquetSharp.Date), parquetDate).SetName("parquetDate->parquetDate");
                yield return new TestCaseData(parquetDate, typeof(global::ParquetSharp.Date), typeof(DateTime), someDate).SetName("parquetDate->DateTime");
                yield return new TestCaseData(parquetDate, typeof(global::ParquetSharp.Date), typeof(DateOnly), DateOnly.FromDateTime(someDate)).SetName("parquetDate->DateOnly");

                var dt = new DateTime(2022, 2, 2, 12, 0, 0, DateTimeKind.Utc);
                yield return new TestCaseData(dt, typeof(DateTime), typeof(DateTime), dt).SetName("DateTime->DateTime");
                yield return new TestCaseData(dt, typeof(DateTime), typeof(DateOnly), DateOnly.FromDateTime(dt)).SetName("DateTime->DateOnly");
                yield return new TestCaseData(dt, typeof(DateTime), typeof(global::ParquetSharp.Date), new global::ParquetSharp.Date(dt)).SetName("DateTime->parquetDate");

                var dOnly = new DateOnly(2023, 3, 3);
                yield return new TestCaseData(dOnly, typeof(DateOnly), typeof(DateOnly), dOnly).SetName("DateOnly->DateOnly");
                yield return new TestCaseData(dOnly, typeof(DateOnly), typeof(DateTime), dOnly.ToDateTime(TimeOnly.MinValue)).SetName("DateOnly->DateTime");
                yield return new TestCaseData(dOnly, typeof(DateOnly), typeof(global::ParquetSharp.Date), new global::ParquetSharp.Date(dOnly.ToDateTime(TimeOnly.MinValue))).SetName("DateOnly->parquetDate");

                yield return new TestCaseData(TimeSpan.FromHours(1.5), typeof(TimeSpan), typeof(TimeSpan), TimeSpan.FromHours(1.5)).SetName("TimeSpan->TimeSpan");

                // Null to nullable succeeds
                yield return new TestCaseData(null, typeof(object), typeof(int?), null).SetName("null->NullableInt");
                yield return new TestCaseData(null, typeof(object), typeof(decimal?), null).SetName("null->NullableDecimal");
                yield return new TestCaseData(null, typeof(object), typeof(long?), null).SetName("null->NullableLong");
            }
        }

        // Failing conversions: expect given exception type
        public static IEnumerable FailingConversions
        {
            get
            {
                yield return new TestCaseData(true, typeof(bool), typeof(int), typeof(InvalidOperationException)).SetName("bool->int_fail");
                yield return new TestCaseData(null, typeof(bool?), typeof(bool), typeof(InvalidOperationException)).SetName("boolNullableNull->bool_fail");

                yield return new TestCaseData(TimeSpan.FromSeconds(5), typeof(TimeSpan), typeof(int), typeof(InvalidOperationException)).SetName("TimeSpan->int_fail");

                yield return new TestCaseData("notanint", typeof(string), typeof(int), typeof(FormatException)).SetName("stringInvalid->int_fail");
                yield return new TestCaseData("notaguid", typeof(string), typeof(Guid), typeof(FormatException)).SetName("stringInvalid->Guid_fail");
                yield return new TestCaseData("", typeof(string), typeof(int), typeof(FormatException)).SetName("emptyString->int_fail");
                yield return new TestCaseData("25:00", typeof(string), typeof(TimeOnly), typeof(FormatException)).SetName("stringBadTime->TimeOnly_fail");
                yield return new TestCaseData("2020-02-30", typeof(string), typeof(DateOnly), typeof(FormatException)).SetName("stringBadDate->DateOnly_fail");

                yield return new TestCaseData("256", typeof(string), typeof(byte), typeof(OverflowException)).SetName("stringOverflow->byte_fail");
                yield return new TestCaseData((decimal)1.23m, typeof(decimal), typeof(float), typeof(InvalidOperationException)).SetName("decimal->float_fail");
                yield return new TestCaseData((double)1.23, typeof(double), typeof(float), typeof(InvalidOperationException)).SetName("double->float_fail");
                yield return new TestCaseData(long.MaxValue, typeof(long), typeof(int), typeof(InvalidOperationException)).SetName("longMax->int_fail");
                yield return new TestCaseData((decimal)5m, typeof(decimal), typeof(ulong), typeof(InvalidOperationException)).SetName("decimal->ulong_fail");
                var g = Guid.NewGuid();
                yield return new TestCaseData(g, typeof(Guid), typeof(string), typeof(InvalidOperationException)).SetName("guidSource->string_fail");

                yield return new TestCaseData(10, typeof(int), typeof(short), typeof(InvalidOperationException)).SetName("int->short_fail");
                yield return new TestCaseData(10, typeof(int), typeof(uint), typeof(InvalidOperationException)).SetName("int->uint_fail");
                yield return new TestCaseData((uint)10, typeof(uint), typeof(int), typeof(InvalidOperationException)).SetName("uint->int_fail");
                yield return new TestCaseData(1.0f, typeof(float), typeof(int), typeof(InvalidOperationException)).SetName("float->int_fail");
                yield return new TestCaseData(1.0d, typeof(double), typeof(int), typeof(InvalidOperationException)).SetName("double->int_fail");

                yield return new TestCaseData(null, typeof(object), typeof(int), typeof(InvalidOperationException)).SetName("null->int_fail");
                yield return new TestCaseData(null, typeof(object), typeof(DateTime), typeof(InvalidOperationException)).SetName("null->DateTime_fail");
                yield return new TestCaseData(null, typeof(object), typeof(Guid), typeof(InvalidOperationException)).SetName("null->Guid_fail");
                yield return new TestCaseData(null, typeof(string), typeof(string), typeof(InvalidOperationException)).SetName("null->string_fail");
            }
        }

        [Test]
        [TestCaseSource(nameof(SuccessfulConversions))]
        public void Convert_Success(object? source, Type sourceType, Type targetType, object? expected)
        {
            var actual = InvokeConvert(source, sourceType, targetType);
            AssertEqualWithTolerance(expected, actual, targetType);
        }

        [Test]
        [TestCaseSource(nameof(FailingConversions))]
        public void Convert_Failure(object? source, Type sourceType, Type targetType, Type expectedExceptionType)
        {
            TestDelegate call = () => InvokeConvert(source, sourceType, targetType);
            Assert.That(call, Throws.InstanceOf(expectedExceptionType));
        }
    }
}
