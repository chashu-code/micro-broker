package utils

import "syscall"

func IsProcessExist(pid int) bool {
	return syscall.Kill(pid, 0) == nil
}
