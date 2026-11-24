CS 171, Fall 2026
Final Project: The Paxos Playlist Predicament
Assigned: Nov 4, 2026. Due: Dec 6, 2026
Teams of Two
Demos: Monday Dec 7, 2026
1 Overall Description
5 students had a great time together studying for CS171, so to celebrate the end of the quarter, they will
attend a music festival together over winter break. In preparation for the festival, they want to make a
shared playlist of songs that each of them can download onto their iPods for their own listening convenience.
However, these friends are fierce debaters, and they could not reach consensus on even a single song after
hours of discussion. Drawing on ideas from their favorite class from the recently-finished fall quarter, they
designed a Paxos-based protocol to facilitate their voting procedure. You are tasked with implementing this
protocol for them.
2 Functional Specification
Formally, a song is represented by a 5-digit positive integer value. A playlist is a list of songs, represented
by an unordered set of integers. Each song may appear at most once in any playlist. Initially, this playlist
is empty. Each person has a preference for each song: Likes (L), Dislikes (D), or Indifferent (I).
Alongside the playlist, there is also a change log describing operations being applied to the playlist. The
entries in the change log (log entries) are indexed with integers starting from 0. Initially, the change log is
empty (so the first available log index is 0). The possible operations are:
1. ADD s: Adds song s to the playlist successfully
2. DELETE s: Deletes song s from the playlist successfully. The song must exist in the playlist.
ADD s can only be performed if song s is not currently in the playlist, and DELETE s can only be performed
if song s is already in the playlist. A student may only propose an operation that can be performed.
At any point in time, a student’s playlist must be the same as the result of applying all operations on their
changelog in order. The students will achieve consensus on the change log, and apply the decided operations
to their individual playlists. If the students share the same change log, then the resulting playlists must also
be the same.
1
For consensus, the students use a modified variant of Paxos to replicate the change log. When a student wants
to propose an operation O in the log, they will attempt to replicate O in the smallest-available log index
that has not been decided yet in the change log:
1. Select ballot num to be higher than any ballot that the student has seen for log index. Broadcast
PREPARE with ⟨ballot num, log index⟩.
2. When a student receives PREPARE, reply PROMISE with ⟨ballot num, log index, accept num, accept op⟩
if ballot num is higher than the student’s current promised ballot.
3. Upon receiving PROMISE from > 1/3 of all nodes (2 nodes, including itself), student n becomes a
proposer. If n learns about other accepted operation(s), choose the one with highest accept num and
set O to accept op. Otherwise, n must gather votes for O before proceeding:
• Broadcast VOTE REQUEST with ⟨ballot num, log index, O⟩
• Receiving nodes respond with VOTE REPLY with ⟨ballot num, log index, O, vote⟩, where vote
reflects the responding student’s vote for the proposed operation.
• If n receives YES votes from 3 nodes (including itself), it may proceed with operation O in the
ACCEPT. If n receives 3 NO votes or a reasonable amount of time has passed (> 20 seconds)
without achieving enough YES votes. the proposal fails and student n does not proceed with
the ACCEPT phase for O.
4. Broadcast ACCEPT with ⟨ballot num, log index, O⟩.
5. If a receiving student has not promised to a higher ballot num for the current log index, they accept
⟨ballot num, log index, O⟩ and reply ACCEPTED with ⟨ballot num, log index, O⟩
6. Upon receiving ACCEPTED from > 2/3 of all nodes (4 nodes, including itself), student n broadcasts
DECIDE with ⟨ballot num, log index, O⟩.
It is possible for multiple operations to compete for the same log index. Only one of these operations can
be committed at that index, and the other operations should not be replicated at that index. Additionally,
a student will not automatically repropose the losing operation.
Each person has an id k, and it’s preference for a song is determined by the k-th digit of the song number
from the left. Person k LIKES the song if the digit is 1 or 2. Person k DISLIKES the song if the digit is 3
or 4. Otherwise, Person k is indifferent to the song. A student’s behavior is described as follows:
• If a student LIKES the song, it will vote YES to ADD operations and NO to DELETE operations,
and can only propose ADD operations for the song.
• If a student DISLIKES the song, it will vote YES to DELETE operations and NO to ADD operations,
and can only propose DELETE operations for the song.
• If a student neither likes nor dislikes the song, it will always vote YES, and it cannot propose any
operations for the song.
Example song: 19403
• Person 1 LIKES the song, because the 1st digit is a 1.
• Persons 3 and 5 DISLIKE the song, because the 3rd digit is 4 and the 5th digit is 3
• Persons 2 and 4 are indifferent to the song.
2
3 User Interface
Each student should implement the following APIs, which are executed by receiving a command message
over the network. The format of this command message is up to you, but these APIs must accept the
arguments described below.
propose_song(k: int, song: int, op: ADD | DELETE):
- Person k proposes the operation {op song}
pause(k: int)
- Person k loses network connection and stops sending/receiving messages.
The process remains running and retains it’s current state.
resume(k: int)
- Person k regains network connection and starts sending/receiving messages
Each person k should maintain three files:
• playlist_k.txt: Person k’s playlist. Print one song per line, and print all songs sorted in increasing
order.
• changes_k.log: Person k’s change log. For each operation, print one line containing {index} {OP} {song}.
Example:
0 ADD 12345
1 ADD 21413
2 DELETE 12345
3 ADD 54321
• messages_k.log: A log of all messages sent and received by person k.
4 Correctness Criteria
• When calling propose song once in isolation and the vote succeeds, the operation is applied to the
playlist. If the vote fails instead, the operation is never proposed.
• When submitting two operations for the same log entry, only one of them can be successfully replicated.
• Once an operation is committed, it cannot be overwritten by any other node.
• The system is still able to replicate operations on the change log with up to one node failure, including
if the proposer fails.
• All copies of the changelog & playlist should be kept as up-to-date as possible. If a student discovers
that its log is behind another student’s log, it should use some mechanism to catch up. It is up to you
to determine how a node can discover if it is behind and fill in it’s missing log entries, as long as you
understand your recovery process.
In order to properly observe these criteria, please add a large random delay to all network communication
(3-6 seconds, for example). Similar to PA1, you must use socket programming for networking.
3
5 Demos
• To streamline code demos, we would like you to also provide a Makefile that can be used when demon-
strating the functionality of the code. The required commands are:
make start # Start all 5 student processes
make add NODE={k} SONG={song} # Student k proposes ‘ADD song‘
make delete NODE={k} SONG={song} # Student k proposes ‘DELETE song‘
make pause NODE={k} # Pause student k
make resume NODE={k} # Resume paused student k
make killall # Kill all running student processes
make clean # Delete all log + output files
One way to do this is to have the Makefile call a CLI script that sends commands to running nodes
through client socket connections.
• In this project, you can form teams of 2. You can pick your partner, but both of you are equally
responsible for the entire project and understanding all parts. If your partner bails out before the end,
you will be responsible for the entire project–pick your partner wisely and reliably.
• Demos will be held all day on Monday December 7, 2026. The demos will be held In Person. We will
specify the exact location later. You are responsible to understand all parts of the project. You should
be able to explain your code fully without excessive use of comments. You are not allowed to use any
form of AI tools during the demo. You must come to the demo. The entire grade is based on the demo.
Please submit your code to Gradescope.