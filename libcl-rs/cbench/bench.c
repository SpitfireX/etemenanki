#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <time.h>

#include <cwb/cl.h>

int main(int argc, char **argv) {
    // open test corpus
    char *path = "testdata/registry";
    char *name = "simpledickens";
    Corpus *c = cl_new_corpus(path, name);
    assert(c != NULL);

    // open p attribute
    Attribute *attr = cl_new_attribute(c, "word", ATT_POS);
    assert(attr != NULL);

    int max = cl_max_cpos(attr);
    assert(max > 0);

    int len = 0;
    int runs = 10;

    time_t start = clock();
    // decode complete attribute
    for (int r = 0; r < runs; r++) {
        for (int i = 0; i < max; i++) {
            char *str = cl_cpos2str(attr, i);
            len += strlen(str);
        }
    }
    time_t elapsed = clock() - start;

    printf("total chars: %i\n", len);
    printf("ns per iteration: %i\n", (elapsed/runs)*1000);
}
