def print_test_failed(test, note):
    print("\033[0;37;41m TEST FAILED:\033[0m \033[1;31;40m {}\033[0m".format(test))
    print("\033[1;31;40mNote: {}\033[0m".format(note))

def print_test_passed(test):
    print("\033[0;37;42m TEST PASSED:\033[0m  {} ".format(test))