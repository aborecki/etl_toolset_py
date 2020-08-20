import colored
def test(value, expected_value, message ):
    if value == expected_value:
        print(colored.attr("bold") + colored.bg("green") + "TEST OK: "+message+colored.attr(0))


    else:
        print(colored.attr("bold") + colored.bg("red") + "TEST FAILED: "+message+colored.attr(0))
        print(colored.attr("bold") + colored.fg("red") + "\nEXPECTED VALUE:\n" + str(expected_value) +
              "\nTESTED VALUE:\n" + str(value)+colored.attr(0))