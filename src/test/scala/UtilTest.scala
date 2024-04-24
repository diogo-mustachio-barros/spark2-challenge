// import org.scalatest._
import org.scalatest.funsuite.AnyFunSuite

class UtilTest extends AnyFunSuite {

    val DELTA = 0.0001
    def compareDoubles(x: Double, y: Double): Boolean = {
        (x - y).abs < DELTA
    }

    test("Util.parseSizeInMB.variesWithDevice") {
        assert(Util.parseSizeInMB("Varies with device") == None)
    }

    test("Util.parseSizeInMB.gb") {
        val res = Util.parseSizeInMB("1.2G")
        assert(res.isDefined)
        assert(compareDoubles(res.get, 1200.0))
    }

    test("Util.parseSizeInMB.mb") {
        val res = Util.parseSizeInMB("1.2M")
        assert(res.isDefined)
        assert(compareDoubles(res.get, 1.2))
    }

    test("Util.parseSizeInMB.kb") {
        val res = Util.parseSizeInMB("1.2k")
        assert(res.isDefined)
        assert(compareDoubles(res.get, 0.0012))
    }

    test("Util.parseDollarPrice.zero") {
        val res = Util.parseDollarPrice("0")
        assert(res.isDefined)
        assert(compareDoubles(res.get, 0))
    }

    test("Util.parseDollarPrice.valid") {
        val res = Util.parseDollarPrice("$4.99")
        assert(res.isDefined)
        assert(compareDoubles(res.get, 4.99))
    }

    test("Util.parseDollarPrice.invalid") {
        assert(Util.parseDollarPrice("") == None)
    }
}
