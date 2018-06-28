; noop.asm
BITS 32
GLOBAL _start
SECTION .text
_start:
	mov eax, 1 ; exit syscall
	mov ebx, 0 ; exit code
	int 0x80 ; run syscall

