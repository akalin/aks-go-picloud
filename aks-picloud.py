import argparse
import cloud
import json
import math
import subprocess


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
    # TODO(akalin): Calculate M ourselves.
    parser.add_argument('M', type=int, help='the AKS upper bound')
    args = parser.parse_args()

    if not args.j:
        args.j = int(math.sqrt(args.M))

    step = args.M // args.j
    def find_aks_witness_for_n(start):
        return find_aks_witness(args.n, start, start + step)

    start_range = xrange(1, args.M, step)
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
