# Paxos Paper Impeccable Notes

This file tracks design critiques and follow-up distill/polish work for the
Part-Time Parliament paper.

## Current Direction

- Paper tone: retro-futurist lecture notes
- Selective energy: retro-futurist magazine moments at chapter openings,
  transitions, hero moments, and demo framing
- Non-goals: generic dark dashboard, loud neon poster, cluttered interaction

## Critiques

- [ ] Preserve the vaporwave identity after the landing page instead of dropping
      into a generic dark technical shell.
      Command: `normalize`
- [ ] Distill the navigation so it feels like an editorial table of contents,
      while keeping some of the paper's vaporwave atmosphere.
      Command: `distill`
- [ ] Define a quieter reading-mode expression of the vaporwave palette for
      whitepapers, overview pages, and section pages.
      Command: `colorize`
- [ ] Strengthen the typographic voice of the reading pages so the paper feels
      authored, not just themed.
      Command: `frontend-design`
- [ ] Improve paper-level continuity between landing, whitepapers, reading
      pages, and demos.
      Command: `normalize`
- [ ] Reduce repeated heavy container usage across overview and section pages so
      hierarchy comes more from layout and type than from stacked dark panels.
      Command: `distill`
- [ ] Improve reading rhythm on section pages by varying section treatment
      instead of repeating quote box plus explanation in the same cadence.
      Command: `normalize`
- [ ] Make quote treatment feel curated rather than procedural so key Lamport
      passages stand out from supporting excerpts.
      Command: `polish`
- [ ] Rework the overview page into more of an annotated paper map and less of
      a categorized link list in blocks.
      Command: `critique`
- [ ] Make preliminary demo SVG motion more semantic so message types, failures,
      and success states read clearly without relying on the event log.
      Command: `animate`

## Current Notes

- The latest navbar pass improved usability, but it stripped too much of the
  vaporwave character.
- Overview and section pages are readable, but they still feel more like
  competent notes than a fully authored editorial teaching document.
- Quote treatment now supports a stronger `anchor-quote` variant for lead
  Lamport passages, while keeping ordinary excerpts quieter and more supporting.
