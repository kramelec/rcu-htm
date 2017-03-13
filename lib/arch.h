#ifndef _ARCH_H_
#define _ARCH_H_

#if defined(__powerpc__) || defined(__ppc__) || defined(__PPC__)
#	define __POWERPC__
#endif

#if defined(__powerpc64__) || defined(__ppc64__) || defined(__PPC64__)
#	define __POWERPC64__
#endif

#if defined(__POWERPC64__)
#	define CACHE_LINE_SIZE 128
#else
#	define CACHE_LINE_SIZE 64
#endif

#endif /* _ARCH_H_ */
