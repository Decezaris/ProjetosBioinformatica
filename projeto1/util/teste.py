def generator_test():

    s = []
    ex = list(range(8))

    for i in ex:
        s += ex
        #yield s

    yield 'ola'

s = generator_test()
print(next(s))
