## Design Context

### Users
This project is for students and mid-level engineers learning distributed systems.
It exists to explain difficult papers in a more modern, usable, intuitive way than
the original math-heavy presentations, while supporting those explanations with
interactive demos that make the ideas concrete.

### Brand Personality
Subtle, refined, efficient, with a bit of quirkiness.
The interface should feel thoughtful and intentional rather than loud, generic, or
over-produced.

### Aesthetic Direction
The overall product direction is editorial and mobile first.
Each paper should have its own distinct visual flavor and voice, but the site should
still feel coherent at the structural level through consistent usability, navigation,
and interaction discipline.

This project does not need to force a global light-mode or dark-mode framing.
Each paper can define its own tonal palette and atmosphere if it remains readable,
usable, and aligned with the teaching goal.

### Design Principles
1. Readability and usability come first. Every visual decision should support learning,
   comprehension, and navigation before style.
2. Treat demos as teaching instruments, not spectacle. Motion and interaction should
   clarify state and behavior, not decorate for its own sake.
3. Keep the interface restrained. Avoid clutter, kitchen-sink layouts, gratuitous
   interactions, and over-the-top animation.
4. Avoid sterile minimalism. The work should be clean and efficient without feeling
   stripped down, empty, or generic.
5. Preserve a shared editorial baseline across the site while allowing each paper to
   develop its own mood, typography, palette, and visual identity.

### Writing Tone
- Write like a sharp engineer explaining a hard paper to another engineer, not like
  a marketer, professor, or product copywriter.
- Be direct, plainspoken, and willing to say when the original paper is confusing,
  badly explained, or overly theatrical.
- Keep a little bite and personality. Mild irreverence is good. Forced charm,
  brand cheerfulness, and faux-grand language are not.
- Prefer concrete explanations over abstraction. Name what the thing is, why it
  matters, and where the paper makes life harder than it needs to.
- Use short, active sentences when possible. Keep the signal high. Cut filler,
  hedging, and generic framing.
- Humor should be sparse and purposeful. It should clarify or puncture pretension,
  not turn the site into a running bit.
- Do not write like "AI explainer copy". Avoid polished but empty transitions,
  repetitive scaffolding, and generic phrases like "bridge", "journey", "dive into",
  "pull back the curtain", or "under the hood" unless they are truly the best wording.

### Copy Editing Rule
- Do not rewrite existing copy outside `templates/paxos/whitepapers.html` unless the
  user explicitly asks for copy changes on that page or section first.
