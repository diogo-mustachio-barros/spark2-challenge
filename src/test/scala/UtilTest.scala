// import org.scalatest._
import org.scalatest.funsuite.AnyFunSuite

class UtilTest extends AnyFunSuite {
    
    class withAlmostEquals(d:Double) {
        def ~=(y: Double)(implicit precision: Double) = (d - y).abs <= precision
    }
    implicit val precision = 0.00001
    implicit def add_~=(d:Double) = new withAlmostEquals(d)

    test("Util.parseSizeInMB.variesWithDevice") {
        assert(Util.parseSizeInMB("Varies with device") == None)
    }

    test("Util.parseSizeInMB.gb") {
        val res = Util.parseSizeInMB("1.2G")
        assert(res.isDefined)
        assert(res.get ~= 1200.0)
    }

    test("Util.parseSizeInMB.mb") {
        val res = Util.parseSizeInMB("1.2M")
        assert(res.isDefined)
        assert(res.get ~= 1.2)
    }

    test("Util.parseSizeInMB.kb") {
        val res = Util.parseSizeInMB("1.2k")
        assert(res.isDefined)
        assert(res.get ~= 0.0012)
    }
}
