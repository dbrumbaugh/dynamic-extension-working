/*
 *
 */

#define ENABLE_TIMER
#define TS_TEST

#include <thread>

#include "file_util.h"
#include "framework/interface/Record.h"
#include "framework/structure/MutableBuffer.h"
#include "shard/ISAMTree.h"
#include "standard_benchmarks.h"

#include "psu-util/timer.h"
#include <gsl/gsl_rng.h>

typedef de::Record<uint64_t, uint64_t> Rec;
typedef de::ISAMTree<Rec> Shard;
typedef de::MutableBuffer<Rec> Buffer;

void usage(char *progname) {
  fprintf(stderr, "%s reccnt datafile\n", progname);
}

int main(int argc, char **argv) {

  if (argc < 3) {
    usage(argv[0]);
    exit(EXIT_FAILURE);
  }

  size_t n = atol(argv[1]);
  std::string d_fname = std::string(argv[2]);

  auto data = read_sosd_file<Rec>(d_fname, n);

  std::vector<size_t> buffer_sizes = {4000,  8000,  12000, 16000,
                                      20000, 40000, 80000, 160000, 320000};

  TIMER_INIT();

  size_t rec_idx = 0;

  for (size_t bs : buffer_sizes) {
    for (size_t j = 0; j < 20; j++) {
      auto buffer = Buffer(bs, bs);

      TIMER_START();
      for (size_t i = 0; i < bs; i++) {
        buffer.append(data[rec_idx++]);

        if (rec_idx >= n) rec_idx = 0;
      }
      TIMER_STOP();

      auto buffer_fill = TIMER_RESULT();

      TIMER_START();
      auto shard = Shard(buffer.get_buffer_view());
      TIMER_STOP();

      auto shard_const = TIMER_RESULT();

      fprintf(stdout, "%ld\t%ld\t%ld\n", bs, buffer_fill, shard_const);
    }
  }
}
