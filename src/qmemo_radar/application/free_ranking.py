from collections.abc import Callable, Sequence
from datetime import UTC, datetime

from qmemo_radar.application.ports import SelectionRepository
from qmemo_radar.application.selection import ClusterSignals, SelectionPolicy, free_score
from qmemo_radar.domain import EventCandidate, ScoreResult


class FreeRanker:
    """The ranker when no paid LLM is enabled: the story's preselection score, explained.

    No network, no model, no cost. An event outside any story (a manual link) is scored as a
    single mention.
    """

    def __init__(
        self,
        repository: SelectionRepository,
        policy: SelectionPolicy,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._repository = repository
        self._policy = policy
        self._clock = clock

    async def rank(self, events: Sequence[EventCandidate]) -> list[ScoreResult]:
        now = self._clock()
        found = await self._repository.cluster_signals([e.event_id for e in events], now=now)
        by_id = {signals.event.event_id: signals for signals in found}
        return [
            free_score(
                by_id.get(event.event_id) or ClusterSignals(event=event), self._policy, now=now
            )
            for event in events
        ]
