import argparse
import cloud
import fractions
import json
import math
import subprocess
import sys


def calculate_euler_phi_prime_power(p, k):
    '''Assuming p is prime, calculates and returns Phi(p^k) quickly.
    '''
    return (p - 1) * p**(k - 1)


def trial_divide(n, upper_bound=None):
    '''Does trial division to find factors of n and yields them. If
    upper_bound is set, only factors less than or equal to it will be
    tried.
    '''

    # Factors out d from t as much as possible and calls factor_fn if
    # d divides t.
    def factor_out(d, t, upper_bound):
        m = 0
        while t % d == 0:
            t /= d
            upper_bound = min(upper_bound, t)
            m += 1

        return (m, t, upper_bound)

    if not upper_bound:
        upper_bound = int(math.sqrt(n))

    t = n

    # Try small primes first.
    for i in [2, 3, 5, 7]:
        if i < upper_bound:
            (m, t, upper_bound) = factor_out(i, t, upper_bound)
            if m != 0:
                yield (i, m)

    # Then run through a mod-30 wheel, which cuts the number of odd
    # numbers to test roughly in half.
    mod_30_wheel = [4, 2, 4, 2, 4, 6, 2, 6]
    i = 1
    d = 11
    while d <= upper_bound:
        (m, t, upper_bound) = factor_out(d, t, upper_bound)
        if m != 0:
            yield (d, m)

        d += mod_30_wheel[i]
        i = (i + 1) % len(mod_30_wheel)

    if t != 1:
        yield (t, 1)


def calculate_multiplicative_order_prime_power(a, p, k):
    '''Assuming that p is prime and a and p^k are coprime, returns the
    smallest power e of a such that a^e = 1 (mod p^k).
    '''
    n = p**k
    t = calculate_euler_phi_prime_power(p, k)

    factors = []

    if k > 1:
        factors += [(p, k - 1)]

    factors += trial_divide(p - 1)

    o = 1
    for (q, e) in factors:
        # Calculate x = a^(t/q^e) (mod n).
        x = pow(a, t/q**e, n)

        while x != 1:
            o *= q
            x = pow(x, q, n)

    return o


def calculate_multiplicative_order(a, n):
    '''Assuming that a and n are coprime, returns the smallest power e of
    a such that a^e = 1 (mod n).
    '''
    o = 1

    for (q, e) in trial_divide(n):
        oq = calculate_multiplicative_order_prime_power(a, q, e)
        # Set o to lcm(o, oq).
        gcd = fractions.gcd(o, oq)
        o = (o * oq) / gcd

    return o


def calculate_euler_phi(n):
    '''Calculate Phi(n) by factorizing it.
    '''
    phi = 1

    for (q, e) in trial_divide(n):
        phi *= calculate_euler_phi_prime_power(q, e)

    return phi


def calculate_aks_modulus_upper_bound(n):
    '''Returns an upper bound for r such that o_r(n) > ceil(lg(n))^2 that
    is polylog in n.
    '''
    ceil_lg_n = n.bit_length()
    r_upper_bound = max(ceil_lg_n**5, 3)

    if n % 8 == 0 or n % 5 == 0:
        r_upper_bound = min(r_upper_bound, 8 * ceil_lg_n**2)

    return r_upper_bound


def calculate_aks_modulus(n):
    '''Returns the least r such that o_r(n) > ceil(lg(n))^2 >= ceil(lg(n)^2).
    '''
    ceil_lg_n_sq = n.bit_length()**2
    upper_bound = calculate_aks_modulus_upper_bound(n)
    for r in xrange(ceil_lg_n_sq + 2, upper_bound):
        if fractions.gcd(n, r) != 1:
            continue
        o = calculate_multiplicative_order(n, r)
        if o > ceil_lg_n_sq:
            return r

    raise Exception('Could not find AKS modulus for %d' % n)


def calculate_aks_upper_bound(n, r):
    '''Returns floor(sqrt(Phi(r))) * ceil(lg(n)) + 1 >
    floor(sqrt(Phi(r))) * lg(n).
    '''
    ceil_lg_n = n.bit_length()
    return int(math.sqrt(calculate_euler_phi(r))) * ceil_lg_n + 1


def find_aks_witness(n, start, end):
    '''Shell out to aks-picloud-worker to test primality and return
    the result as a dictionary.
    '''
    result_str = subprocess.check_output(
        ['/home/picloud/src/aks-picloud/aks-picloud-worker/aks-picloud-worker',
         '-start=%d' % start,
         '-end=%d' % end,
         str(n)])
    result = json.loads(result_str)
    result['n'] = int(result['n'])
    result['r'] = int(result['r'])
    result['M'] = int(result['M'])
    result['start'] = int(result['start'])
    result['end'] = int(result['end'])
    if 'factor' in result:
        result['factor'] = int(result['factor'])
    if 'witness' in result:
        result['witness'] = int(result['witness'])
    return result


def main():
    parser = argparse.ArgumentParser(
        description='Test primality via the AKS algorithm.')
    parser.add_argument('-j', type=int, help='the number of jobs to use')
    parser.add_argument('n', type=int, help='the number to test')
    args = parser.parse_args()

    if args.n < 2:
        sys.stderr.write("n must be >= 2\n")
        sys.exit(-1)

    r = calculate_aks_modulus(args.n)
    M = calculate_aks_upper_bound(args.n, r)

    if not args.j:
        args.j = int(math.sqrt(M))

    print 'n = %d, r = %d, M = %d' % (args.n, r, M)

    for (q, e) in trial_divide(args.n, M - 1):
        if q < args.n:
            print '%d has factor %d' % (args.n, q)
            return

    # M^2 > N iff M > floor(sqrt(N)).
    if M**2 > args.n:
        print '%d is prime' % args.n
        return

    step = M // args.j
    def find_aks_witness_for_n(start):
        return find_aks_witness(args.n, start, start + step)

    start_range = xrange(1, M, step)
    print 'calling into PiCloud with %d jobs...' % len(start_range)

    jids = cloud.map(find_aks_witness_for_n, start_range,
                     _env='aks', _type='f2', _cores=4,
                     _label='find_aks_witness(%d)' % args.n)

    print 'waiting for results from %d jobs...' % len(jids)
    results = cloud.result(jids)
    # TODO(akalin): Accumulate non-witnesses.
    for result in results:
        if 'isPrime' in result:
            print '%d is prime' % args.n
            break
        elif 'factor' in result:
            print '%d has factor %d' % (args.n, result['factor'])
            break
        elif 'witness' in result:
            print '%d has witness %d' % (args.n, result['witness'])
            break
    else:
        print '%d is prime' % args.n


if __name__ == '__main__':
    main()
