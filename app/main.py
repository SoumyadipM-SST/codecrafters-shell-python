import sys
import os
import subprocess
import threading
from pathlib import Path

current_directory = Path(os.getcwd()).absolute()
background_jobs = []
BUILTINS = ["echo", "type", "exit", "pwd", "cd", "jobs"]

class BackgroundJob:
    def __init__(self, job_number, pid, base_command, status, pipeline_threads, pipeline_processes):
        self.job_number = job_number
        self.pid = pid
        self.base_command = base_command
        self.status = status
        self.pipeline_threads = pipeline_threads
        self.pipeline_processes = pipeline_processes

    def is_alive(self):
        for p in self.pipeline_processes:
            if p.poll() is None:
                return True
        for t in self.pipeline_threads:
            if t.is_alive():
                return True
        return False

def main():
    global current_directory
    
    while True:
        reap_completed_jobs(sys.stdout, True)
        
        sys.stdout.write("$ ")
        sys.stdout.flush()
        
        try:
            input_line = sys.stdin.readline()
            if not input_line:
                break
        except EOFError:
            break
            
        input_line = input_line.strip()
        if not input_line:
            continue
            
        tokens = parse_arguments(input_line)
        if not tokens:
            continue
            
        is_background = False
        if tokens[-1] == "&":
            is_background = True
            tokens.pop()
            
        if not tokens:
            continue
            
        # --- Check for Pipelines ---
        if "|" in tokens:
            handle_pipeline(tokens, is_background)
            continue
            
        execute_single_command(tokens, is_background, None, None)


def handle_pipeline(tokens, is_background):
    global background_jobs, current_directory
    
    commands_tokens = []
    current_cmd = []
    for token in tokens:
        if token == "|":
            if current_cmd:
                commands_tokens.append(current_cmd)
                current_cmd = []
        else:
            current_cmd.append(token)
    if current_cmd:
        commands_tokens.append(current_cmd)

    num_cmds = len(commands_tokens)
    processes = []
    threads = []

    pipes = []
    for _ in range(num_cmds - 1):
        try:
            r, w = os.pipe()
            pipes.append((r, w))
        except Exception as e:
            sys.stdout.write(f"Pipe creation failed: {e}\n")
            sys.stdout.flush()
            return

    for i in range(num_cmds):
        cmd_args = commands_tokens[i]
        command = cmd_args[0]

        stage_in_fd = 0 if i == 0 else pipes[i - 1][0]
        stage_out_fd = 1 if i == num_cmds - 1 else pipes[i][1]

        if command in BUILTINS:
            stage_idx = i
            
            def builtin_thread_func(args, in_fd, out_fd, s_idx):
                in_stream = sys.stdin if in_fd == 0 else os.fdopen(in_fd, 'r', encoding='utf-8', closefd=True)
                out_stream = sys.stdout if out_fd == 1 else os.fdopen(out_fd, 'w', encoding='utf-8', closefd=True)
                
                try:
                    execute_builtin(args, in_stream, out_stream, sys.stderr)
                finally:
                    if out_fd != 1:
                        try: out_stream.close()
                        except: pass
                    if in_fd != 0:
                        try: in_stream.close()
                        except: pass

            t = threading.Thread(target=builtin_thread_func, args=(cmd_args, stage_in_fd, stage_out_fd, stage_idx))
            threads.append(t)
            t.start()
        else:
            exec_path = get_executable_path(command)
            if not exec_path:
                sys.stdout.write(f"{command}: command not found\n")
                sys.stdout.flush()
                return
            
            try:
                p = subprocess.Popen(
                    cmd_args,
                    cwd=str(current_directory),
                    stdin=stage_in_fd if stage_in_fd != 0 else None,
                    stdout=stage_out_fd if stage_out_fd != 1 else None,
                    stderr=None  # inherits naturally
                )
                processes.append(p)
            except Exception as e:
                sys.stdout.write(f"Error starting pipeline process: {e}\n")
                sys.stdout.flush()

            if stage_in_fd != 0:
                try: os.close(stage_in_fd)
                except: pass
            if stage_out_fd != 1:
                try: os.close(stage_out_fd)
                except: pass

    if is_background:
        assigned_job_id = 1
        if background_jobs:
            assigned_job_id = max(j.job_number for j in background_jobs) + 1
            
        rep_pid = processes[-1].pid if processes else threading.get_ident()
        sys.stdout.write(f"[{assigned_job_id}] {rep_pid}\n")
        sys.stdout.flush()
        
        base_cmd_str = " ".join(tokens)
        background_jobs.append(BackgroundJob(assigned_job_id, rep_pid, base_cmd_str, "Running", threads, processes))
    else:
        for p in processes:
            p.wait()
        for t in threads:
            t.join()


def execute_builtin(cmd_tokens, in_stream, out_stream, err_stream):
    command = cmd_tokens[0]
    
    def write_out(s):
        out_stream.write(s + "\n")
        out_stream.flush()

    if command == "echo":
        write_out(" ".join(cmd_tokens[1:]))
    elif command == "type":
        if len(cmd_tokens) < 2:
            return
        check_str = cmd_tokens[1]
        if check_str in BUILTINS:
            write_out(f"{check_str} is a shell builtin")
        else:
            executable_path = get_executable_path(check_str)
            if executable_path:
                write_out(f"{check_str} is {executable_path}")
            else:
                write_out(f"{check_str}: not found")
    elif command == "pwd":
        write_out(str(current_directory))
    elif command == "jobs":
        reap_completed_jobs(out_stream, False)
    elif command == "exit":
        sys.exit(0)


def execute_single_command(tokens, is_background, custom_in, custom_out):
    global current_directory, background_jobs
    
    redirect_file = None
    redirect_err_file = None
    append_stdout = False
    append_stderr = False
    redirect_index = -1

    for i, token in enumerate(tokens):
        if token in (">>", "1>>"):
            if i + 1 < len(tokens):
                redirect_file = tokens[i + 1]
                append_stdout = True
                redirect_index = i
                break
        elif token in (">", "1>"):
            if i + 1 < len(tokens):
                redirect_file = tokens[i + 1]
                append_stdout = False
                redirect_index = i
                break
        elif token == "2>>":
            if i + 1 < len(tokens):
                redirect_err_file = tokens[i + 1]
                append_stderr = True
                redirect_index = i
                break
        elif token == "2>":
            if i + 1 < len(tokens):
                redirect_err_file = tokens[i + 1]
                append_stderr = False
                redirect_index = i
                break

    cmd_tokens = tokens[:redirect_index] if redirect_index != -1 else tokens
    if not cmd_tokens:
        return

    command = cmd_tokens[0]

    stdout_target = custom_out if custom_out is not None else sys.stdout
    out_file_obj = None
    if redirect_file:
        try:
            f_path = Path(redirect_file)
            if f_path.parent: f_path.parent.mkdir(parents=True, exist_ok=True)
            out_file_obj = open(f_path, 'a' if append_stdout else 'w')
            stdout_target = out_file_obj
        except IOError:
            pass

    stderr_target = sys.stderr
    err_file_obj = None
    if redirect_err_file:
        try:
            f_path = Path(redirect_err_file)
            if f_path.parent: f_path.parent.mkdir(parents=True, exist_ok=True)
            err_file_obj = open(f_path, 'a' if append_stderr else 'w')
            stderr_target = err_file_obj
        except IOError:
            pass

    try:
        if command in BUILTINS:
            if command == "cd":
                if len(cmd_tokens) < 2:
                    return
                target_path_str = cmd_tokens[1]
                if target_path_str == "~":
                    target_path = Path(os.environ.get("HOME", ""))
                elif target_path_str.startswith("~/"):
                    target_path = Path(os.environ.get("HOME", "")) / target_path_str[2:]
                elif target_path_str.startswith("/"):
                    target_path = Path(target_path_str)
                else:
                    target_path = current_directory / target_path_str
                    
                target_path = target_path.absolute().resolve()
                if target_path.exists() and target_path.is_dir():
                    current_directory = target_path
                else:
                    stderr_target.write(f"cd: {target_path_str}: No such file or directory\n")
                    stderr_target.flush()
            else:
                execute_builtin(cmd_tokens, custom_in if custom_in else sys.stdin, stdout_target, stderr_target)
        else:
            executable_path = get_executable_path(command)
            if executable_path:
                try:
                    base_command_str = " ".join(tokens)
                    
                    p = subprocess.Popen(
                        cmd_tokens,
                        cwd=str(current_directory),
                        stdin=custom_in,
                        stdout=stdout_target,
                        stderr=stderr_target
                    )

                    if is_background:
                        assigned_job_id = 1
                        if background_jobs:
                            assigned_job_id = max(j.job_number for j in background_jobs) + 1
                        sys.stdout.write(f"[{assigned_job_id}] {p.pid}\n")
                        sys.stdout.flush()
                        background_jobs.append(BackgroundJob(assigned_job_id, p.pid, base_command_str, "Running", [], [p]))
                    else:
                        p.wait()
                except Exception as e:
                    sys.stdout.write(f"Error executing command : {e}\n")
                    sys.stdout.flush()
            else:
                sys.stdout.write(f"{command}: command not found\n")
                sys.stdout.flush()
    finally:
        if out_file_obj:
            out_file_obj.close()
        if err_file_obj:
            err_file_obj.close()


def reap_completed_jobs(target_stream, only_print_done):
    global background_jobs
    
    for job in background_jobs:
        if job.status == "Running" and not job.is_alive():
            job.status = "Done"

    output = []
    total_jobs = len(background_jobs)

    for i in range(total_jobs):
        job = background_jobs[i]
        if only_print_done and job.status != "Done":
            continue

        marker = ' '
        if i == total_jobs - 1: marker = '+'
        elif i == total_jobs - 2: marker = '-'

        display_command = job.base_command if job.status == "Done" else job.base_command + " &"
        output.append(f"[{job.job_number}]{marker}  {job.status:<24}{display_command}\n")

    background_jobs = [job for job in background_jobs if job.status != "Done"]

    if output:
        target_stream.write("".join(output))
        target_stream.flush()


def parse_arguments(input_str):
    tokens = []
    current_token = []
    inside_single_quotes = False
    inside_double_quotes = False
    token_started = False
    
    i = 0
    while i < len(input_str):
        c = input_str[i]
        if inside_single_quotes:
            if c == "'": inside_single_quotes = False
            else: current_token.append(c)
            token_started = True
        elif inside_double_quotes:
            if c == '\\':
                if i + 1 < len(input_str):
                    next_char = input_str[i + 1]
                    if next_char in ('"', '\\'):
                        current_token.append(next_char)
                        i += 1
                    else:
                        current_token.append(c)
                else:
                    current_token.append(c)
            elif c == '"':
                inside_double_quotes = False
            else:
                current_token.append(c)
            token_started = True
        else:
            if c == '\\':
                if i + 1 < len(input_str):
                    i += 1
                    current_token.append(input_str[i])
                    token_started = True
            elif c == "'":
                inside_single_quotes = True
                token_started = True
            elif c == '"':
                inside_double_quotes = True
                token_started = True
            elif c.isspace():
                if token_started:
                    tokens.append("".join(current_token))
                    current_token.clear()
                    token_started = False
            else:
                current_token.append(c)
                token_started = True
        i += 1
        
    if token_started:
        tokens.append("".join(current_token))
    return tokens


def get_executable_path(command):
    path_env = os.environ.get("PATH")
    if path_env is not None:
        directories = path_env.split(os.pathsep)
        for directory in directories:
            if not directory: 
                continue
            full_path = Path(directory) / command
            if full_path.exists() and full_path.is_file() and os.access(full_path, os.X_OK):
                return str(full_path.absolute())
    return None


if __name__ == "__main__":
    main()
